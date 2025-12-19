import json
import logging
import re
import os
import google.generativeai as genai
from typing import List, Dict, Any

# ==============================================================================
# 1. REFINED UTILITY: GEMINI CLIENT
# ==============================================================================

# TODO: Ensure this env var is set in your Airflow setup
GEMINI_API_KEY = os.getenv("GOOGLE_API_KEY", "YOUR_ACTUAL_API_KEY_HERE")
GEMINI_MODEL = "gemini-2.5-pro"  # Use Variable.get() in actual DAG

genai.configure(api_key=GEMINI_API_KEY)

def get_gemini_response(
    prompt: str,
    system_instruction: str = None,
    conversation_history=None,
    temperature: float = 0.7
) -> str:
    """
    Call Google Gemini model with a specific system instruction.
    
    Args:
        prompt: The user input/query.
        system_instruction: The system prompt to guide behavior/persona.
        conversation_history: List of dicts with {"prompt": ..., "response": ...}
        temperature: Creativity level.
    """
    try:
        model = genai.GenerativeModel(GEMINI_MODEL, system_instruction=system_instruction)

        # Build chat history if provided
        chat_history = []
        if conversation_history:
            for turn in conversation_history:
                chat_history.append({"role": "user", "parts": [turn["prompt"]]})
                chat_history.append({"role": "model", "parts": [turn["response"]]})

        chat = model.start_chat(history=chat_history)

        # We prefer JSON response type for stability, even if expect_json param is gone
        # Using "application/json" ensures the model tries to output JSON if requested in prompt
        response = chat.send_message(
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


# ==============================================================================
# 2. SYSTEM PROMPTS (The "Solid" Logic)
# ==============================================================================

# PROMPT 1: LOGICAL SPLITTER
# Focus: Grammar, meaning, and sentence structure.
SYSTEM_PROMPT_SPLITTER = """
You are an expert Script Editor. 
Your task is to take a raw paragraph and break it down into individual thought units or sentences.
Do NOT number them. Do NOT add labels. Just separate the ideas logically.
Return a JSON object: { "draft_segments": ["thought 1", "thought 2"] }
"""

# PROMPT 2: FORMATTER & PACING ENFORCER
# Focus: Timing (8s), word count (15 words), and CLEANING (No "Scene 1").
SYSTEM_PROMPT_FORMATTER = """
You are a Video Production Assistant.
Your task is to finalize script segments for a TTS (Text-to-Speech) engine.

STRICT CONSTRAINTS:
1. **NO LABELS**: Never include "Scene 1", "Narrator:", or "Visual:". Return ONLY the spoken words.
2. **TIMING**: Each string must be read in approx 8 seconds (Max 15 words).
3. **SPLITTING**: If a segment from the draft is too long, split it into two.
4. **OUTPUT**: Return a pure JSON list of strings.
   Example: ["First sentence of audio.", "Second sentence of audio."]
"""


# ==============================================================================
# 3. TASK LOGIC
# ==============================================================================

def validate_and_truncate(segments: List[str]) -> Dict[str, Any]:
    MAX_SCENES = 10
    MAX_WORDS = 15
    warnings = []
    
    # 1. Truncate
    if len(segments) > MAX_SCENES:
        original = len(segments)
        segments = segments[:MAX_SCENES]
        warnings.append(f"Truncated from {original} to {MAX_SCENES} scenes.")
        
    # 2. Validate Content
    clean_segments = []
    for i, seg in enumerate(segments):
        # Final safety cleanup (removing accidentally leaked "Scene:" text)
        clean_seg = re.sub(r'^(Scene\s*\d+|Step\s*\d+|Narrator):?\s*', '', seg, flags=re.IGNORECASE).strip()
        
        # Word count check
        wc = len(clean_seg.split())
        if wc > MAX_WORDS:
            warnings.append(f"Segment {i+1} is {wc} words (Rec: <{MAX_WORDS}). Audio may be fast.")
            
        clean_segments.append(clean_seg)

    return {
        "status": "success",
        "segments": clean_segments,
        "segment_count": len(clean_segments),
        "warnings": warnings
    }

def split_script_task(script_text: str) -> Dict[str, Any]:
    if not script_text or not script_text.strip():
        return {"status": "error", "message": "Input is empty"}

    try:
        # --- CALL 1: ROUGH DRAFT ---
        logging.info("🔹 Step 1: Logical Splitting...")
        draft_json = get_gemini_response(
            prompt=f"Split this text:\n{script_text}",
            system_instruction=SYSTEM_PROMPT_SPLITTER,
            temperature=0.3
        )
        draft_data = json.loads(draft_json)
        draft_segments = draft_data.get("draft_segments", [])
        
        # --- CALL 2: FINALIZE & FORMAT ---
        logging.info("🔹 Step 2: Enforcing 8-second pacing & cleaning...")
        # We pass the draft segments to the second call to refine them
        draft_str = json.dumps(draft_segments)
        
        final_json = get_gemini_response(
            prompt=f"Refine these segments for 8-second video pacing:\n{draft_str}",
            system_instruction=SYSTEM_PROMPT_FORMATTER,
            temperature=0.1 # Low temp for strict formatting
        )
        
        final_segments = json.loads(final_json)
        
        # Check if API returned dict wrapper instead of list
        if isinstance(final_segments, dict):
            final_segments = final_segments.get("segments", list(final_segments.values())[0])

        return validate_and_truncate(final_segments)

    except json.JSONDecodeError:
        logging.error(f"JSON Parsing Error. Raw output: {final_json}")
        return {"status": "error", "message": "AI failed to format JSON"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ==============================================================================
# 4. TEST RUNNER
# ==============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    if "YOUR_ACTUAL" in GEMINI_API_KEY:
        print("❌ ERROR: Set your API Key in line 13")
        exit(1)

    print("-" * 60)
    print("TEST: Script with Metadata (Should be cleaned)")
    print("-" * 60)
    
    # Input with artifacts that we want the AI to remove
    dirty_input = (
        "Scene 1: AI agents are the future. "
        "Narrator: They connect to your database. "
        "Scene 2: This keeps data private. "
        "Visual: Show a lock icon. "
        "And finally, the workflow is automated."
    )
    
    result = split_script_task(dirty_input)
    print(json.dumps(result, indent=2))