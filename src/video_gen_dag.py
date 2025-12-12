from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.models import Variable
import logging
import random
import json
from pathlib import Path
import sys
import re   

# Add the parent directory to Python path
dag_dir = Path(__file__).parent
sys.path.insert(0, str(dag_dir))
from agent.veo import GoogleVeoVideoTool
from models.gemini import get_gemini_response

logger = logging.getLogger(__name__)

GEMINI_API_KEY = Variable.get("video.companion.gemini.api_key")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 12),
    'retries': 1,
}




def build_scene_config(segments_data, aspect_ratio="16:9", images=[]):
    """
    Transforms AI output into downstream config.
    NOW: Uses the 'duration' provided by the AI, with safety clamping.
    """
    MAX_SCENES = 10
    
    # Safety Limits (In case AI hallucinates a 20s or 1s duration)
    ABSOLUTE_MIN = 5.0
    ABSOLUTE_MAX = 10.0
    
    final_config = []
    image_list = images
    
    # 1. Truncate
    if len(segments_data) > MAX_SCENES:
        segments_data = segments_data[:MAX_SCENES]

    for item in segments_data:
        # Extract fields
        text = item.get("text", "").strip()
        ai_duration = float(item.get("duration", 6.0)) # Default to 6s if missing
        
        # Clean text
        text = re.sub(r'^(Narrator|Speaker|Scene \d+):?\s*', '', text, flags=re.IGNORECASE).strip()
        
        # Validate/Clamp Duration (Trust but verify)
        final_duration = max(ABSOLUTE_MIN, min(ai_duration, ABSOLUTE_MAX))
        # Pick ONE random image
        selected_image = random.choice(image_list) if image_list else None
        logger.info(f"Images: {image_list}")
        config_item = {
            "image_path": selected_image,
            "prompt": text,
            "duration": final_duration,
            "aspect_ratio": aspect_ratio
        }
        # logger.info(config_item)
        final_config.append(config_item)

    return final_config


def get_config(**context):
    """Task 1: Retrieve params and push to XCom."""
    ti = context['ti']
    config = {
        'images': context['params']['images'],
        # 'prompt': context['params']['prompt'],
        # 'resolution': context['params']['resolution'],
        # 'duration': context['params']['duration'],
        # 'aspect_ratio': context['params']['aspect_ratio'],
        # 'video_model': Variable.get('CF_VIDEO_MODEL', default_var='mock'),
        "status": "draft",
        'video_meta': {
                "aspect_ratio": "16:9",
                "target_duration": 25
                        },
        "script_content":  context['params']['script_content'],
    }
    ti.xcom_push(key='config', value=config)
    images_list =  ["/appz/home/airflow/dags/image.png"]
    ti.xcom_push(key='images', value=images_list)
    
    logger.info(f"Pushed config to XCom: {config}")

def extract_json_from_text(text):
    """
    Extract JSON from text, supporting both objects {} and arrays [].
    
    Args:
        text: String that may contain JSON data
        
    Returns:
        Parsed JSON (dict or list) or None if extraction fails
    """
    try:
        text = text.strip()
        
        # Remove markdown code blocks
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*', '', text)
        text = text.strip()
        
        # Try direct JSON parsing first (most efficient)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        
        # Try to find JSON object {}
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        
        # Try to find JSON array []
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        
        logging.warning(f"Could not extract JSON from text: {text[:100]}...")
        return None
        
    except Exception as e:
        logging.error(f"JSON extraction error: {e}")
        return None
    
def split_script_task(**context):
        
        ti = context['ti']
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
        3. **ASSIGN DURATION**: For each segment, you must assign a specific duration in seconds.
        - **MINIMUM**: 6 seconds
        - **MAXIMUM**: 8 seconds
        - Logic: Use 6s for shorter punchy lines, 8s for longer descriptive lines.
        - RULE: Do not use fractions only give 6 or 8 seconds as input.
        4. **PAD IF NEEDED**: If a segment cannot be merged, slightly rewrite it to be more descriptive to increase duration.
        * Input: "Mars Horizon."
        * Output: "Witness the revelation of the red planet in Mars Horizon."
        5. **NATURAL FLOW**: Group short, dramatic phrases together.
        * BAD: ["Until now.", "An anomaly."]
        * GOOD: ["Until now, an anomaly waits beneath the sands."]
        6. **OUTPUT FORMAT**: Return a JSON list of objects.
        Example:
        [
            { "text": "Until now, a strange ancient anomaly waits beneath.", "duration": 6 },
            { "text": "Witness the revelation of the red planet.", "duration": 6 }
        ]
        """

        # --- INPUT HANDLING ---
        
        raw_data = ti.xcom_pull(key='config', task_ids='get_config')
        images = ti.xcom_pull(key='images', task_ids='get_config')
        logger.info
        script_content = raw_data.get('script_content')
        
        # Extract Aspect Ratio (Default to 16:9 if missing)
        video_meta = raw_data.get('video_meta', {})
        aspect_ratio = video_meta.get('aspect_ratio', '16:9')

        if not script_content or script_content == "Paste your full script here...":
            logger.error("❌ No script content provided.")
            return [] # Return empty list on error for consistent type

        logger.info(f"📥 Processing Script... (Aspect Ratio: {aspect_ratio})")

        # --- EXECUTION ---
        
        # Step A: Clean Extraction
        draft_json = get_gemini_response(
            prompt=f"Extract spoken lines from:\n{script_content}",
            system_instruction=SYSTEM_PROMPT_EXTRACTOR,
            temperature=0.2,
            api_key=GEMINI_API_KEY
        )
        
        try:
            logger.info(f"Initial Draft : {draft_json}")
            draft_data = extract_json_from_text(draft_json)
            draft_segments = draft_data.get("draft_segments", draft_data)
            if not isinstance(draft_segments, list):
                draft_segments = list(draft_data.values())[0] if isinstance(draft_data, dict) else []
        except:
            logger.error("Failed extraction step")
            return []

        if not draft_segments:
            logger.error("No script lines found")
            return []

        # Step B: Pacing
        final_json = get_gemini_response(
            prompt=f"Optimize these lines for natural video flow (merge short phrases):\n{json.dumps(draft_segments)}",
            system_instruction=SYSTEM_PROMPT_FORMATTER,
            temperature=0.3, 
            api_key=GEMINI_API_KEY
        )
        logger.info(f"FINAL:: {final_json}")
        try:
            final_data = extract_json_from_text(final_json)
            final_segments = final_data.get("segments", final_data) if isinstance(final_data, dict) else final_data
            logger.info(f"Final Data {final_data} ")
            # --- FINAL STEP: BUILD CONFIG OBJECT ---
            segments_for_processing = build_scene_config(segments_data=final_segments, aspect_ratio=aspect_ratio,images=images)
            # logger.info(f"segments: {segments_for_processing} ")
            ti.xcom_push(key="segments",value=segments_for_processing)
            return segments_for_processing

        except Exception as e:
            logger.error(f"Failed pacing step: {e}")
            return []

from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

# Modify process_config to accept a single segment
def process_single_segment(segment, **context):
    """Process ONE segment at a time."""
    ti = context['ti']
    logger.info(f"Processing segment: {segment}")
    
    video_model = Variable.get('CF_video_model', default_var='mock')
    
    if video_model == 'mock':
        mock_list_raw = Variable.get('mock_list', default_var='[]')
        mock_list = json.loads(mock_list_raw) if mock_list_raw else []
        video_path = random.choice(mock_list) if mock_list else None
        logger.info(f"Mock video: {video_path}")
        return video_path
    else:
        # Real Veo generation
        logger.info("Using Google Veo 3.0")
        try:
            veo_tool = GoogleVeoVideoTool(api_key=GEMINI_API_KEY)
            
            result = veo_tool._run(
                image_path=segment.get('image_path'),
                prompt=segment.get('prompt'),
                aspect_ratio=segment.get('aspect_ratio', '16:9'),
                duration_seconds=segment.get('duration', 6),
                output_dir=Variable.get('OUTPUT_DIR', default_var='/appz/home/airflow/dags')
            )
            
            if result.get('success'):
                video_path = result['video_paths'][0]
                logger.info(f"✅ Generated: {video_path}")
                return video_path
            else:
                raise ValueError(f"Generation failed: {result.get('error')}")
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            raise

# Add a helper task to prepare expand data
def prepare_segments_for_expand(**context):
    """Convert segments list into format needed for expand."""
    ti = context['ti']
    segments = ti.xcom_pull(task_ids='split_script', key='segments')
    
    if not segments:
        logger.warning("No segments found!")
        return []
    
    # Return list of dicts for expand
    return [{'segment': seg} for seg in segments]
dag = DAG(
    'sample_config_dag',
    default_args=default_args,
    description='Sample Airflow DAG with config params, XCom, and Airflow Variable',
    schedule_interval=None,
    catchup=False,
    params={
       "images": Param(
            default=[],                                   # default: empty list
            type="array",                                 # ← correct Airflow type
            items={"type": "string"},                     # ← each item is a string (file path)
            minItems=1,                                   # optional: require at least one
            title="Input Images",
            description="Upload one or more images",
        ),
        # 'prompt': Param('', type='string', description='Text prompt for processing'),
        # 'resolution': Param('', type='string', description='Output resolution (e.g., 1024x1024)'),
        # 'duration': Param(0, type='integer', description='Duration in seconds'),
        'aspect_ratio': Param('', type='string', description='Aspect ratio (e.g., 16:9)'),
        "script_content":  Param('', type='string', description='Text prompt for processing'),
    },
    tags=['sample', 'config', 'xcom', 'variable'],
)
# Update your DAG tasks
task1 = PythonOperator(
    task_id='get_config',
    python_callable=get_config,
    dag=dag,
)

task2 = PythonOperator(
    task_id='split_script',
    python_callable=split_script_task,
    dag=dag,
)

task3 = PythonOperator(
    task_id='prepare_segments',
    python_callable=prepare_segments_for_expand,
    dag=dag,
)

# Dynamic mapping - this creates N tasks
process_segments = PythonOperator.partial(
    task_id='process_segment',
    python_callable=process_single_segment,
    dag=dag,
    pool="video_processing_pool"
).expand(op_kwargs=task3.output)

# Set dependencies
task1 >> task2 >> task3 >> process_segments