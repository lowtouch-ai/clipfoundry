import google.generativeai as genai
import json

def get_gemini_response(
    prompt: str,
    system_instruction: str | None = None,
    conversation_history: list[dict[str, str]] | None = None,
    temperature: float = 0.7,
) -> str:
    """
    Call Google Gemini model with optional system instruction and chat history.

    Args:
        prompt: The current user message/query.
        system_instruction: Optional system prompt to guide model behavior/persona.
        conversation_history: List of previous turns → [{"prompt": ..., "response": ...}]
        temperature: Creativity level (0.0 – 1.0).

    Returns:
        Model response as string.
    """
    try:
        # Always request JSON mime type for more reliable structured output
        # (especially useful when system_instruction asks for JSON)
        generation_config = genai.GenerationConfig(
            response_mime_type="application/json",
            temperature=temperature,
        )

        # Create model with system instruction
        model = genai.GenerativeModel(
            model_name=GEMINI_MODEL,
            system_instruction=system_instruction,
            generation_config=generation_config,
        )

        # Build chat history in Gemini's expected format
        chat_history = []
        if conversation_history:
            for turn in conversation_history:
                chat_history.append({"role": "user", "parts": [turn["prompt"]]})
                chat_history.append({"role": "model", "parts": [turn["response"]]})

        # Start chat and send message
        chat = model.start_chat(history=chat_history)
        response = chat.send_message(prompt)

        return response.text.strip()

    except Exception as e:
        logging.error(f"Gemini API error: {e}", exc_info=True)
        error_msg = f"AI request failed: {str(e)}"
        # Return a JSON string on error for consistency with JSON mime type
        return json.dumps({"error": error_msg})