import json
import logging
import re
import os
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param  # <--- NEW IMPORT
import google.generativeai as genai

# ==============================================================================
# CONFIGURATION
# ==============================================================================

GEMINI_API_KEY = os.getenv("GOOGLE_API_KEY", Variable.get("google_api_key", default_var="YOUR_API_KEY_HERE"))
GEMINI_MODEL = Variable.get("video.companion.gemini.model", default_var="gemini-1.5-flash")

genai.configure(api_key=GEMINI_API_KEY)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_gemini_response(prompt: str, system_instruction: str = None, temperature: float = 0.7) -> str:
    """Wrapper for Gemini API calls."""
    try:
        model = genai.GenerativeModel(GEMINI_MODEL, system_instruction=system_instruction)
        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                temperature=temperature,
            )
        )
        return response.text.strip()
    except Exception as e:
        logging.error(f"Gemini API error: {e}", exc_info=True)
        return json.dumps({"error": f"AI request failed: {str(e)}"})

def validate_and_truncate(segments):
    """Business Logic: Max 10 scenes, 15 words/sec limit."""
    MAX_SCENES = 10
    MAX_WORDS = 15
    warnings = []

    if len(segments) > MAX_SCENES:
        segments = segments[:MAX_SCENES]
        warnings.append(f"Truncated to {MAX_SCENES} scenes.")

    clean_segments = []
    for i, seg in enumerate(segments):
        seg = re.sub(r'^(Narrator|Speaker|Scene \d+):?\s*', '', seg, flags=re.IGNORECASE).strip()
        wc = len(seg.split())
        if wc > MAX_WORDS:
            warnings.append(f"Segment {i+1} is {wc} words (Limit {MAX_WORDS}). Audio may be rushed.")
        clean_segments.append(seg)

    return {
        "status": "success",
        "segments": clean_segments,
        "segment_count": len(clean_segments),
        "warnings": warnings
    }

# ==============================================================================
# DAG DEFINITION
# ==============================================================================

default_args = {
    'owner': 'lowtouch-ai',
    'depends_on_past': False,
    'retries': 0,
}

# --- DEFINE DEFAULT PARAMS FOR UI ---
# This ensures the Text Box / JSON Editor appears when you click Trigger
default_json_structure = {
    "script_content": "Paste your full script here...",
    "status": "draft",
    "video_meta": {"target_duration": 25}
}

with DAG(
    dag_id='video_script_splitter_integrated',
    default_args=default_args,
    description='Parses raw script input into 8-second video segments',
    schedule_interval=None,
    start_date=pendulum.today('UTC'),
    catchup=False,
    tags=['video', 'ai'],
    # 🔴 THIS PARAMS BLOCK FORCES THE CONFIG WINDOW TO OPEN 🔴
    params={
        "raw_data": Param(
            default_json_structure, 
            type="object", 
            title="Raw Data JSON",
            description="The input object containing 'script_content'"
        ),
        "markdown_output": Param(
            "", 
            type=["string", "null"],
            title="Markdown Output (Optional)"
        )
    }
) as dag:

    @task(task_id='split_script_content')
    def split_script_task(**kwargs):
        
        # --- PROMPTS ---
        SYSTEM_PROMPT_EXTRACTOR = """
        You are a Script Extraction Specialist.
        Your input includes conversation, sound effects, script lines, and reasoning.
        TASK:
        - Extract ONLY the spoken words (Narration/Dialog).
        - IGNORE "Reasoning for changes", sound effects, and labels.
        - Return JSON: { "draft_segments": ["line 1", "line 2"] }
        """

        SYSTEM_PROMPT_FORMATTER = """
        You are a Video Pacing Expert.
        Refine segments for TTS (Text-to-Speech).

        CRITICAL RULES:
        1. **AVOID FRAGMENTATION**: Do not leave single words (like "Alien.", "Ancient.") as their own segments. Merge them with the previous or next phrase.
        2. **OPTIMAL LENGTH**: Target 6-12 words per segment. (Max 15 words).
        3. **NATURAL FLOW**: Group short, dramatic phrases together.
           * BAD: ["Until now.", "An anomaly."]
           * GOOD: ["Until now, an anomaly waits beneath the sands."]
        4. **FORMAT**: Return a pure JSON list of strings.
        """

        # --- INPUT HANDLING ---
        # With 'params' defined, inputs are guaranteed to be in kwargs['params'] or dag_run.conf
        params = kwargs.get('params', {})
        raw_data = params.get('raw_data', {})
        
        # Fallback to direct dag_run.conf if params proxy is bypassed
        if not raw_data:
             dag_run = kwargs.get('dag_run')
             if dag_run and dag_run.conf:
                 raw_data = dag_run.conf.get('raw_data', {})

        script_content = raw_data.get('script_content')

        if not script_content or script_content == "Paste your full script here...":
            logging.error("❌ No script content provided.")
            return {"status": "error", "message": "Please provide valid script_content in JSON."}

        logging.info(f"📥 Processing Script ({len(script_content)} chars)")

        # --- EXECUTION ---
        # Step A: Clean
        draft_json = get_gemini_response(
            prompt=f"Extract spoken lines from:\n{script_content}",
            system_instruction=SYSTEM_PROMPT_EXTRACTOR,
            temperature=0.2
        )
        
        try:
            draft_data = json.loads(draft_json)
            draft_segments = draft_data.get("draft_segments", draft_data)
            if not isinstance(draft_segments, list):
                 draft_segments = list(draft_data.values())[0] if isinstance(draft_data, dict) else []
        except:
            return {"status": "error", "message": "Failed extraction step"}

        if not draft_segments:
             return {"status": "error", "message": "No script lines found."}

        # Step B: Pace
        final_json = get_gemini_response(
            prompt=f"Refine for 8s pacing:\n{json.dumps(draft_segments)}",
            system_instruction=SYSTEM_PROMPT_FORMATTER,
            temperature=0.1
        )

        try:
            final_data = json.loads(final_json)
            final_segments = final_data.get("segments", final_data) if isinstance(final_data, dict) else final_data
            return validate_and_truncate(final_segments)
        except:
            return {"status": "error", "message": "Failed pacing step"}

    split_script_task()