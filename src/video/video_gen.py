import asyncio
import json
import os
from pathlib import Path
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from langchain.tools import BaseTool
from langgraph.prebuilt import create_react_agent
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.callbacks import CallbackManagerForToolRun
import logging
import base64
import tempfile
import time
from pydantic import BaseModel, Field, validator
from google import genai
from google.genai import types
import uuid


# ==================== MODELS ====================

class VeoVideoInput(BaseModel):
    image_path: str = Field(..., description="Local file path to the person's reference image (face photo)")
    prompt: str = Field(..., description="Exact text the person should speak in the video")
    aspect_ratio: Optional[str] = Field("9:16", description="9:16 (vertical), 16:9 (horizontal), 1:1")
    duration_seconds: Optional[int] = Field(8, ge=2, le=30)
    resolution: Optional[str] = Field("720p", description="480p, 720p, or 1080p")
    output_dir: Optional[str] = Field(None, description="Where to save the video(s). Defaults to temp dir.")
    video_model: Optional[str] = Field("veo-3.0-fast-generate-001",description="video model")
   
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

    def _run(
        self,
        image_path: str,
        prompt: str,
        scene_description: str = "",
        aspect_ratio: str = "9:16",
        duration_seconds: int = 6,
        output_dir: str = "",
        video_model: str = "veo-3.0-fast-generate-001",
        resolution: str = "720p"
    ) -> Dict[str, Any]:

        # output_dir = "/appz/dev/test_agents/output"
        os.makedirs(output_dir, exist_ok=True)

        image_b64 = self._image_to_base64(image_path)

        # Smart prompt builder
        
        full_prompt = f"""

        The person in the reference image speaks clearly and naturally:
        "{prompt}"

        # IMPOTRANT
        - The character should be exactly match the charater in the given image
        """
        

        config = types.GenerateVideosConfig(
            aspect_ratio=aspect_ratio,
            number_of_videos=1,
            duration_seconds=duration_seconds,
            person_generation="ALLOW_ADULT",
            resolution=resolution
        )

        try:
            operation = self.client.models.generate_videos(
                model=video_model,
                prompt=full_prompt.strip(),
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
                logging.info("Video not generated")
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
