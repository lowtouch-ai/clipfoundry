import json
from typing import List, Dict, Any
import logging

# ========================= MOCKED LLM INTERFACE =========================

def get_gemini_response(input_text: str, system_prompt: str, user_prompt: str) -> str:
    """
    Simulates a call to the Gemini API.
    Returns a JSON string representing the list of script segments.
    """
    logging.info("🤖 CALLING GEMINI API (MOCKED)...")
    
    # SCENARIO 1: EMPTY SCRIPT
    if not input_text or not input_text.strip():
        return "[]"

    # SCENARIO 2: LONG SCRIPT (Over 10 segments to test truncation)
    if "LONG_SCRIPT_TEST" in input_text:
        # Simulating 12 segments
        segments = [f"Scene {i}: This is a long segment explanation." for i in range(1, 13)]
        return json.dumps(segments)

    # SCENARIO 3: STANDARD SCRIPT (Normal use case)
    # Simulating a response based on the "AI Agent" summary task
    standard_segments = [
        "AI Agents are not just chatbots. They actually do the work.",
        "They connect to your apps and automate tasks securely onsite.",
        "Lowtouch.ai makes this easy. No code, just results, privately."
    ]
    return json.dumps(standard_segments)

# ========================= SCENE SPLITTER LOGIC =========================

def split_script_task(script_text: str) -> Dict[str, Any]:
    """
    Parses a raw script into 8-second video segments using an LLM.
    
    Constraints:
    - Max 10 scenes (Truncates with warning if exceeded).
    - Rejects empty scripts.
    - Each segment target duration: ~8 seconds (enforced via Prompt Engineering).
    """
    logging.info("🎬 STARTING TASK: Script Splitting")

    # 1. Validation: Empty Script
    if not script_text or not script_text.strip():
        error_msg = "❌ Error: Input script is empty. Cannot generate video segments."
        logging.error(error_msg)
        return {"status": "error", "message": error_msg, "segments": []}

    # 2. Construct Prompts
    # We instruct the LLM to split based on natural pauses that fit an 8-second speaking window.
    # Approx 12-15 words per segment for a slow, clear pace.
    system_prompt = (
        "You are an expert video scriptwriter. Your task is to split the input text into "
        "distinct video scenes. \n"
        "RULES:\n"
        "1. Each scene must be spoken in approximately 8 seconds (roughly 10-15 words).\n"
        "2. Do not change the meaning, but you may slightly edit for flow.\n"
        "3. Return ONLY a valid JSON list of strings. Example: [\"Segment 1 text\", \"Segment 2 text\"]"
    )
    
    user_prompt = f"Split this script: {script_text}"

    # 3. Call LLM
    try:
        response_str = get_gemini_response(script_text, system_prompt, user_prompt)
        segments = json.loads(response_str)
    except json.JSONDecodeError:
        logging.error("Failed to parse LLM response.")
        return {"status": "error", "message": "LLM output formatting failed", "segments": []}

    # 4. Validation: Too Long (Max 10 scenes)
    MAX_SCENES = 10
    warning = None
    
    if len(segments) > MAX_SCENES:
        original_count = len(segments)
        segments = segments[:MAX_SCENES]
        warning = f"⚠️ Warning: Script was too long ({original_count} scenes). Truncated to {MAX_SCENES} max."
        logging.warning(warning)
    
    # 5. Success Return
    logging.info(f"✅ Successfully split script into {len(segments)} segments.")
    return {
        "status": "success",
        "warning": warning,
        "segments": segments,
        "segment_count": len(segments)
    }

# ========================= TEST RUNNER =========================

if __name__ == "__main__":
    print("--- TEST CASE 1: Standard Script ---")
    input_standard = "AI Agents are not just chatbots..."
    result_1 = split_script_task(input_standard)
    print(json.dumps(result_1, indent=2))
    
    print("\n--- TEST CASE 2: Empty Script ---")
    input_empty = ""
    result_2 = split_script_task(input_empty)
    print(json.dumps(result_2, indent=2))
    
    print("\n--- TEST CASE 3: Too Long Script ---")
    input_long = "LONG_SCRIPT_TEST content..."
    result_3 = split_script_task(input_long)
    print(json.dumps(result_3, indent=2))