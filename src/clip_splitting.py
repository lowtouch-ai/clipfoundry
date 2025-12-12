import json
import logging
import re
import os
import math
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

def build_scene_config(segments, aspect_ratio="16:9"):
    """
    Transforms raw text segments into the configuration objects required by downstream tasks.
    
    Output Structure:
    [
      {
        'image_path': None,  # Workaround: Null for now
        'prompt': "Segment text...",
        'duration': 5.5,     # Calculated
        'aspect_ratio': "16:9"
      },
      ...
    ]
    """
    MAX_SCENES = 10
    # Constraints for valid duration
    MIN_DURATION = 3.0 
    MAX_DURATION = 8.0 
    WORDS_PER_SEC = 2.0 # Slower, cinematic pacing
    
    final_config = []
    
    # 1. Truncate strictly
    if len(segments) > MAX_SCENES:
        logging.warning(f"⚠️ Input had {len(segments)} segments. Truncating to {MAX_SCENES}.")
        segments = segments[:MAX_SCENES]

    for i, seg in enumerate(segments):
        # Clean text
        text = re.sub(r'^(Narrator|Speaker|Scene \d+):?\s*', '', seg, flags=re.IGNORECASE).strip()
        
        # Calculate Duration
        word_count = len(text.split())
        calc_duration = round(word_count / WORDS_PER_SEC, 1)
        
        # Clamp Duration (Keep it between 3s and 8s)
        duration = max(MIN_DURATION, min(calc_duration, MAX_DURATION))
        
        # Build Item
        item = {
            "image_path": None,  # <--- Workaround as requested
            "prompt": text,
            "duration": duration,
            "aspect_ratio": aspect_ratio
        }
        final_config.append(item)

    return final_config

# ==============================================================================
# DAG DEFINITION
# ==============================================================================

default_args = {
    'owner': 'lowtouch-ai',
    'depends_on_past': False,
    'retries': 0,
}

# Default UI input structure
default_json_structure = {
    "script_content": "Paste your full script here...",
    "status": "draft",
    "video_meta": {
        "target_duration": 25,
        "aspect_ratio": "16:9" # Added aspect ratio field
    }
}

with DAG(
    dag_id='video_script_splitter_integrated',
    default_args=default_args,
    description='Parses raw script input into downstream video configuration',
    schedule_interval=None,
    start_date=pendulum.today('UTC'),
    catchup=False,
    tags=['video', 'ai'],
    params={
        "raw_data": Param(
            default_json_structure, 
            type="object", 
            title="Raw Data JSON",
            description="The input object containing 'script_content' and 'video_meta'"
        ),
        "markdown_output": Param("", type=["string", "null"])
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
        
        # Extract Aspect Ratio (Default to 16:9 if missing)
        video_meta = raw_data.get('video_meta', {})
        aspect_ratio = video_meta.get('aspect_ratio', '16:9')

        if not script_content or script_content == "Paste your full script here...":
            logging.error("❌ No script content provided.")
            return [] # Return empty list on error for consistent type

        logging.info(f"📥 Processing Script... (Aspect Ratio: {aspect_ratio})")

        # --- EXECUTION ---
        
        # Step A: Clean Extraction
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
             logging.error("Failed extraction step")
             return []

        if not draft_segments:
             logging.error("No script lines found")
             return []

        # Step B: Pacing
        final_json = get_gemini_response(
            prompt=f"Optimize these lines for natural video flow (merge short phrases):\n{json.dumps(draft_segments)}",
            system_instruction=SYSTEM_PROMPT_FORMATTER,
            temperature=0.3 
        )

        try:
            final_data = json.loads(final_json)
            final_segments = final_data.get("segments", final_data) if isinstance(final_data, dict) else final_data
            
            # --- FINAL STEP: BUILD CONFIG OBJECT ---
            return build_scene_config(final_segments, aspect_ratio=aspect_ratio)

        except Exception as e:
            logging.error(f"Failed pacing step: {e}")
            return []

    split_script_task()