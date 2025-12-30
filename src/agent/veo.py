import os
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from langchain.tools import BaseTool

import logging
import base64
import time
from pydantic import BaseModel, Field
from google import genai
from google.genai import types
import uuid

VEO_SYSTEM_PROMPT = """
You are a professional podcast host creating an Instagram-style educational video.
Speak at a natural, energetic pace.
PERFORMANCE INSTRUCTIONS:
1. Start speaking immediately at frame 0.
2. When the script ends, hold a neutral, professional expression. DO NOT nod, smile excessively, or gasp for air after the last word.
3. Keep the delivery tight. No long pauses between sentences.
4. Maintain eye contact.
"""

STRICT_NEGATIVE_PROMPT = "**Do not include any text, subtitles, watermarks, or logos. Avoid blurry footage, distorted or morphing visuals, and oversaturated neon colors. The video must not have shaky camera movement or a stock-footage aesthetic. Ensure there are no facial artifacts, extra limbs, or lip-sync mismatches. Exclude all music, background noise, and robotic voices; ensure audio is pristine. Do not include Western Christmas imagery like Santa or fake snow, and avoid cultural stereotypes. When the script ends, hold a neutral, professional expression. DO NOT nod, smile excessively, or gasp for air after the last word.**"

# ==================== MODELS ====================

class VeoVideoInput(BaseModel):
    frame1_path: str = Field(..., description="Local file path to the person's reference image (face photo)")
    prompt: str = Field(..., description="Exact text the person should speak in the video")
    frame2_path: Optional[str] = Field(None, description="Optional second frame for slight motion")
    aspect_ratio: Optional[str] = Field("9:16", description="9:16 (vertical), 16:9 (horizontal), 1:1")
    duration_seconds: Optional[int] = Field(..., ge=2, le=30)
    resolution: Optional[str] = Field("720p", description="480p, 720p, or 1080p")
    output_dir: Optional[str] = Field(None, description="Where to save the video(s). Defaults to temp dir.")
    video_model: Optional[str] = Field("veo-3.0-fast-generate-001",description="video model")
    voice_persona: Optional[str] = Field("Professional voice", description="Consistent voice description")
    negative_prompt: Optional[str] = Field("", description="Negative prompt to avoid unwanted elements")
   
class GoogleVeoVideoTool(BaseTool):
    name: str = "google_veo_generate_video_with_scene"
    description: str = (
        "Generate a hyper-realistic or stylized video using Google Veo 3.0 from a reference face image. "
        "The person will speak your text with perfect lip sync. "
        "You have FULL control over scene, background, camera, lighting, clothing, and cinematic style. "
        "Best for TikTok, YouTube Shorts, ads, storytelling, and avatars."
    )
    args_schema: type[BaseModel] = VeoVideoInput
    
    # Declare these as Pydantic fields with default None
    client: Optional[Any] = Field(default=None, exclude=True)
    model: str = Field(default="veo-3.0-fast-generate-001", exclude=True)

    def __init__(self, api_key: Optional[str] = None, **kwargs):
        # Don't pass client/model to super().__init__
        super().__init__(**kwargs)
        
        # Now set them using object.__setattr__ to bypass Pydantic validation
        api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")

        object.__setattr__(self, 'client', genai.Client(
            http_options={"api_version": "v1beta"}, 
            api_key=api_key
        ))
        object.__setattr__(self, 'model', "veo-3.0-fast-generate-001")

    def _image_to_base64(self, path: str) -> str:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Image not found: {path}")
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    def _construct_meta_prompt(self, raw_script: str, character_desc: str, context: str, continuity: str) -> str:
        """
        Constructs the strict 7-Component Meta Prompt for Veo 3.
        """
        # Component 1: Subject (From Character Analysis Task)
        subject_block = f"Subject: {character_desc}"

        # Component 2: Action (From Continuity + Script context)
        # We assume specific lip-sync is handled by model, but we define body language.
        action_block = f"Action: {continuity}. The character speaks naturally."

        # Component 3: Scene (Extracted or Default)
        # We append the specific camera anchor required by Veo 3 framework
        scene_block = f"Scene: {context} (thats where the camera is)."

        # Component 4: Style (Cinematography)
        style_block = "Style: Cinematic lighting, 50mm lens, shallow depth of field, high fidelity, 1080p, broadcast quality."

        # Component 5: Dialogue (Strict Colon Syntax to prevent subtitles)
        # "Character: 'Line'" format
        dialogue_block = f'Dialogue: The character looks at the camera and says: "{raw_script}"'

        # Component 6: Sounds (Prevent Hallucinations)
        # Explicitly separate speech from environment
        sounds_block = "Sounds: Clear speech only. No background music. Room tone matches environment."

        # Assemble the Block
        final_prompt = f"""
{subject_block}

{action_block}

{scene_block}

{style_block}

{dialogue_block}

{sounds_block}
        """
        return final_prompt.strip()

    def _run(
        self,
        frame1_path: str,
        prompt: str,
        frame2_path: str = "",
        scene_description: str = "",
        aspect_ratio: str = "9:16",
        duration_seconds: int = 6,
        output_dir: str = "",
        video_model: str = "veo-3.0-fast-generate-001",
        resolution: str = "720p",
        continuity_context: str = "",
        voice_persona: str = "Professional voice",
        scene_context: str = "Professional studio lighting", # New arg passed from Airflow
        negative_prompt: str = "",
    ) -> Dict[str, Any]:

        # output_dir = "/appz/dev/test_agents/output"
        os.makedirs(output_dir, exist_ok=True)

        image_b64 = self._image_to_base64(frame1_path)

        # 1. Construct the 7-Component Meta Prompt
        # We bypass the LLM "optimizer" and build it deterministically for control
        meta_prompt = self._construct_meta_prompt(
            raw_script=prompt,
            character_desc=voice_persona,
            context=scene_context,
            continuity=continuity_context
        )
        
        logging.info(f"🚀 Veo 3 Meta Prompt:\n{meta_prompt}")
        
        # 2. Configure Veo 3
        config = types.GenerateVideosConfig(
            aspect_ratio=aspect_ratio,
            number_of_videos=1,
            duration_seconds=duration_seconds,
            person_generation="ALLOW_ADULT",
            resolution=resolution,
            negative_prompt = (negative_prompt + ", " + STRICT_NEGATIVE_PROMPT) if negative_prompt else STRICT_NEGATIVE_PROMPT
        )

        try:
            operation = self.client.models.generate_videos(
                model=video_model,
                prompt=meta_prompt,
                image=types.Image(image_bytes=image_b64, mime_type="image/jpeg"),
                config=config,
            )

            # Poll with timeout
            timeout = 420
            start = time.time()
            while not operation.done:
                if time.time() - start > timeout:
                    raise TimeoutError("Veo 3.0 generation timed out after 7 minutes")
                time.sleep(7)
                operation = self.client.operations.get(operation)

            result = operation.result
            if not result or not getattr(result, "generated_videos", None):
                logging.info(f"Video not generated, {result}")
                raise ValueError("No video returned from Veo")
            generated_videos = result.generated_videos
            saved_paths: List[str] = []
            for n, generated_video in enumerate(generated_videos):
                # Download and save video
                unique_id = str(uuid.uuid4())
                downloaded_file = self.client.files.download(file=generated_video.video)
                output_path = os.path.join(f"{output_dir}", f"video_{unique_id}.mp4")
                generated_video.video.save(output_path)
                
                logging.info(f"Video saved to {output_path}")
                saved_paths.append(output_path)

            return {
                "success": True,
                "video_paths": saved_paths,
                "count": len(saved_paths),
                "scene_used": scene_description[:150] + "..." if len(scene_description) > 150 else scene_description or "(default talking head)",
                "message": f"Generated {len(saved_paths)} video(s) with custom scene",
            }

        except Exception as e:
            logging.error(f"Error occurred: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "video_paths": [],
                "message": "Video generation failed",
            }
    async def _arun(self, *args, **kwargs):
        return self._run(*args, **kwargs)
