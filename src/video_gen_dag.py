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

# Add the parent directory to Python path
dag_dir = Path(__file__).parent
sys.path.insert(0, str(dag_dir))
from agent.veo import GoogleVeoVideoTool

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 12),
    'retries': 1,
}

dag = DAG(
    'sample_config_dag',
    default_args=default_args,
    description='Sample Airflow DAG with config params, XCom, and Airflow Variable',
    schedule_interval=None,
    catchup=False,
    params={
        'image_path': Param('', type='string', description='Path to the input image'),
        'prompt': Param('', type='string', description='Text prompt for processing'),
        'resolution': Param('', type='string', description='Output resolution (e.g., 1024x1024)'),
        'duration': Param(0, type='integer', description='Duration in seconds'),
        'aspect_ratio': Param('', type='string', description='Aspect ratio (e.g., 16:9)'),
    },
    tags=['sample', 'config', 'xcom', 'variable'],
)

def get_config(**context):
    """Task 1: Retrieve params and push to XCom."""
    ti = context['ti']
    config = {
        'image_path': context['params']['image_path'],
        'prompt': context['params']['prompt'],
        'resolution': context['params']['resolution'],
        'duration': context['params']['duration'],
        'aspect_ratio': context['params']['aspect_ratio'],
        'video_model': Variable.get('CF_VIDEO_MODEL', default_var='mock'),
    }
    ti.xcom_push(key='config', value=config)
    logger.info(f"Pushed config to XCom: {config}")


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

def process_config(**context):
    """Task 2: Pull config from XCom and process (sample action)."""
    ti = context['ti']
    config = ti.xcom_pull(key='config', task_ids='get_config')
    if not config:
        raise ValueError("No config found in XCom")
    mock_list_raw = Variable.get('mock_list', default_var='mock')
    # Sample processing logic (e.g., validate or simulate work)
    logger.info(f"Processing with config: {config}")
    # Example: Simulate image/video processing based on video_model
    video_model = Variable.get('CF_video_model', default_var='mock')
    if video_model == 'mock':
        mock_list = json.loads(mock_list_raw) if mock_list_raw else []
        video_path= random.choice(mock_list)
        logger.info(f"Returning path: {video_path} {mock_list}")
        return video_path
        logger.info("Using mock video model for simulation.")
    else:
        
        # Real Veo video generation
        logger.info("Using Google Veo 3.0 for video generation")
        
        try:
        
            GEMINI_API_KEY = Variable.get('GEMINI_API_KEY', default_var='')
            # Initialize the tool
            veo_tool = GoogleVeoVideoTool(api_key=GEMINI_API_KEY)
            
            # Extract parameters from config
            image_path = config.get('image_path', 'test_scripts/image.png')
            text = config.get('prompt', 'Hello! This is a generated video.')
            aspect_ratio = config.get('aspect_ratio', '16:9')
            duration_seconds = config.get('duration_seconds', 6)
            output_dir=Variable.get('OUTPUT_DIR', default_var='/appz/home/airflow/dags')
            logger.info(f"Generating video with params: image={image_path}, text={text[:50]}...")
            
            # Generate video
            result = veo_tool._run(
                image_path=image_path,
                prompt=text,
                aspect_ratio=aspect_ratio,
                duration_seconds=duration_seconds,
                output_dir=output_dir
            )
            
            if result.get('success'):
                video_paths = result.get('video_paths', [])
                if video_paths:
                    video_path = video_paths[0]  # Use first generated video
                    logger.info(f"✅ Video generated successfully: {video_path}")
                    
                    # Push video path to XCom for downstream tasks
                    ti.xcom_push(key='generated_video_path', value=video_path)
                    ti.xcom_push(key='veo_result', value=result)
                    
                    return video_path
                else:
                    raise ValueError("No video paths returned from Veo")
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"❌ Video generation failed: {error_msg}")
                raise ValueError(f"Veo generation failed: {error_msg}")
        
        except Exception as e:
            logger.error(f"Exception during video generation: {str(e)}")
            raise
        logger.info(f"Using video model: {video_model}")
        
        
        
        
    
    if config['duration'] > 0:
        logger.info(f"Generating video of duration {config['duration']}s at {config['aspect_ratio']} with prompt: {config['prompt']}")
    else:
        logger.info(f"Generating static image at {config['resolution']} with prompt: {config['prompt']}")
    
    # In a real scenario, this could call external APIs, process files, etc.

task1 = PythonOperator(
    task_id='get_config',
    python_callable=get_config,
    dag=dag,
)

task2 = PythonOperator(
    task_id='process_config',
    python_callable=process_config,
    dag=dag,
)

task1 >> task2