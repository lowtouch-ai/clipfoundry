from google import genai
from google.genai import types
import json
import logging

def get_gemini_response(
    prompt: str,
    api_key: str,
    system_instruction: str | None = None,
    conversation_history: list[dict[str, str]] | None = None,
    temperature: float = 0.7,
    model: str = "gemini-2.5-pro",
) -> str:
    """
    Call Google Gemini model with optional system instruction and chat history.

    Args:
        prompt: The current user message/query.
        api_key: Gemini API key.
        system_instruction: Optional system prompt to guide model behavior/persona.
        conversation_history: List of previous turns → [{"prompt": ..., "response": ...}]
        temperature: Creativity level (0.0 – 1.0).
        model_name: Model to use (default: gemini-2.5-pro).

    Returns:
        Model response as string.
    """
    try:
        # Create client instance with API key
        client = genai.Client(api_key=api_key)

        # Build generation config with system instruction
        config_dict = {
            "response_mime_type": "application/json",
            "temperature": temperature,
        }
        
        # Add system instruction if provided
        if system_instruction:
            config_dict["system_instruction"] = system_instruction
        
        generation_config = types.GenerateContentConfig(**config_dict)

        # Build chat history in Gemini's expected format
        contents = []
        if conversation_history:
            for turn in conversation_history:
                contents.append(
                    types.Content(
                        role="user",
                        parts=[types.Part(text=turn["prompt"])]
                    )
                )
                contents.append(
                    types.Content(
                        role="model",
                        parts=[types.Part(text=turn["response"])]
                    )
                )

        # Add current prompt
        contents.append(
            types.Content(
                role="user",
                parts=[types.Part(text=prompt)]
            )
        )
        logging.info(f"Gemini request contents: {contents}")
        # Generate response with proper system instruction support
        response = client.models.generate_content(
            model=model,
            contents=contents,
            config=generation_config,
        )

        return response.text.strip()

    except Exception as e:
        logging.error(f"Gemini API error: {e}", exc_info=True)
        error_msg = f"AI request failed: {str(e)}"
        # Return a JSON string on error for consistency with JSON mime type
        return json.dumps({"error": error_msg})