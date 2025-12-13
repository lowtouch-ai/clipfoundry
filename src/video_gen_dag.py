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
# Import email utilities
from email_utils.email import (
    authenticate_gmail,
    send_email,
    mark_message_as_read
)

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
        
        # Validate/Clamp Duration
        final_duration = max(ABSOLUTE_MIN, min(ai_duration, ABSOLUTE_MAX))
        # Pick ONE random image
        selected_image = random.choice(image_list) if image_list else None
        
        config_item = {
            "image_path": selected_image,
            "prompt": text,
            "duration": final_duration,
            "aspect_ratio": aspect_ratio
        }
        final_config.append(config_item)

    return final_config


def get_config(**context):
    """Task 1: Retrieve params and push to XCom."""
    ti = context['ti']
    
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    config = {
        'images': conf.get("images", {}),
        'video_meta': {
            "aspect_ratio": conf.get("aspect_ratio", "16:9"),
            "resolution": conf.get("resolution", "720p"),
        },
        "script_content": conf.get("script_content", ""),
    }
    ti.xcom_push(key='config', value=config)
    images_list = conf.get("images", {})
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
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*', '', text)
        text = text.strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        
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
    script_content = raw_data.get('script_content')
    
    # Extract Aspect Ratio (Default to 16:9 if missing)
    video_meta = raw_data.get('video_meta', {})
    aspect_ratio = video_meta.get('aspect_ratio', '16:9')

    if not script_content or script_content == "Paste your full script here...":
        logger.error("❌ No script content provided.")
        return []

    logger.info(f"📥 Processing Script... (Aspect Ratio: {aspect_ratio})")
    
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
        prompt=f"Optimize these lines for natural video flow:\n{json.dumps(draft_segments)}",
        system_instruction=SYSTEM_PROMPT_FORMATTER,
        temperature=0.3, 
        api_key=GEMINI_API_KEY
    )
    
    try:
        final_data = extract_json_from_text(final_json)
        final_segments = final_data.get("segments", final_data) if isinstance(final_data, dict) else final_data
        
        segments_for_processing = build_scene_config(
            segments_data=final_segments, 
            aspect_ratio=aspect_ratio, 
            images=images
        )
        
        ti.xcom_push(key="segments", value=segments_for_processing)
        return segments_for_processing

    except Exception as e:
        logger.error(f"Failed pacing step: {e}")
        return []

def process_single_segment(segment, segment_index, **context):
    """
    Process ONE segment at a time.
    CRITICAL: segment_index preserves the ORDER of video generation.
    """
    ti = context['ti']
    logger.info(f"Processing segment {segment_index}: {segment}")
    
    video_model = Variable.get('CF_video_model', default_var='mock')
    
    if video_model == 'mock':
        mock_list_raw = Variable.get('mock_list', default_var='[]')
        mock_list = json.loads(mock_list_raw) if mock_list_raw else []
        video_path = random.choice(mock_list) if mock_list else None
        logger.info(f"Mock video {segment_index}: {video_path}")
        
        # Return dict with index to preserve order
        return {
            'index': segment_index,
            'video_path': video_path
        }
    else:
        logger.info(f"Using Google Veo 3.0 for segment {segment_index}")
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
                logger.info(f"✅ Generated segment {segment_index}: {video_path}")
                
                return {
                    'index': segment_index,
                    'video_path': video_path
                }
            else:
                raise ValueError(f"Generation failed: {result.get('error')}")
        except Exception as e:
            logger.error(f"Exception in segment {segment_index}: {str(e)}")
            raise

def prepare_segments_for_expand(**context):
    """Convert segments list into format needed for expand with index tracking."""
    ti = context['ti']
    segments = ti.xcom_pull(task_ids='split_script', key='segments')
    
    if not segments:
        logger.warning("No segments found!")
        return []
    
    # Return list of dicts with both segment and index for ordering
    return [
        {
            'segment': seg,
            'segment_index': idx
        } 
        for idx, seg in enumerate(segments)
    ]

def collect_and_merge_videos(**context):
    """
    Collects all generated videos in correct order and triggers merge.
    This task runs AFTER all process_segment tasks complete.
    """
    ti = context['ti']
    
    # Get all results from the mapped tasks
    # Note: When using expand(), results are returned as a list
    segment_results = ti.xcom_pull(task_ids='process_segment')
    
    if not segment_results:
        logger.error("❌ No video segments were generated!")
        raise ValueError("No videos to merge")
    
    logger.info(f"📦 Collected {len(segment_results)} video results")
    
    # Sort by index to maintain correct order
    sorted_results = sorted(segment_results, key=lambda x: x['index'])
    
    # Extract video paths in order
    video_paths = [result['video_path'] for result in sorted_results]
    
    logger.info(f"📹 Video paths in order: {video_paths}")
    
    # Validate all videos exist
    valid_paths = [p for p in video_paths if p and Path(p).exists()]
    
    if len(valid_paths) < len(video_paths):
        logger.warning(f"⚠️ Some videos are missing. Expected {len(video_paths)}, found {len(valid_paths)}")
    
    if len(valid_paths) < 2:
        logger.error("❌ Need at least 2 videos to merge")
        raise ValueError("Insufficient videos for merge")
    
    # Generate request ID for merge
    req_id = context['dag_run'].run_id or f"merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Push to XCom for merge task
    merge_params = {
        'video_paths': valid_paths,
        'req_id': req_id
    }
    
    ti.xcom_push(key='merge_params', value=merge_params)
    
    logger.info(f"✅ Ready to merge {len(valid_paths)} videos with req_id: {req_id}")
    
    return merge_params

def merge_videos_wrapper(**context):
    """
    Wrapper to call the existing merge logic from video_merger_pipeline.
    """
    ti = context['ti']
    
    # Get merge parameters
    merge_params = ti.xcom_pull(task_ids='collect_videos', key='merge_params')
    
    if not merge_params:
        raise ValueError("No merge parameters found")
    
    logger.info(f"🎬 Starting merge with params: {merge_params}")
    
    # Import the merge logic from the other DAG
    # You might need to adjust the import path based on your structure
    from ffmpg_merger import merge_videos_logic
    
    # Create a modified context with params
    modified_context = context.copy()
    modified_context['params'] = merge_params
    
    # Call the merge function
    result = merge_videos_logic(**modified_context)
    ti.xcom_push(key="generated_video_path", value=result)
    logger.info(f"✅ Merge complete! Output: {result}")
    
    return result


def send_video_email(**kwargs):
    """Send generated video to user."""
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    email_data = conf.get("email_data", "")
    message_id = conf.get("message_id", "")
    video_path = ti.xcom_pull(key="generated_video_path", task_ids="merge_all_videos")
    # video_success = ti.xcom_pull(key="video_generation_status", task_ids="merge_all_videos")
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    sender_name = "there"
    name_match = re.search(r'^([^<]+)', sender_email)
    if name_match:
        sender_name = name_match.group(1).strip()
    
    subject = headers.get("Subject", "Video Generation Request")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    
    if video_path and os.path.exists(video_path):
        html_content = f"""
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
        }}
        .greeting {{
            margin-bottom: 15px;
        }}
        .message {{
            margin: 15px 0;
        }}
        .success-box {{
            background-color: #d4edda;
            border-left: 4px solid #28a745;
            padding: 15px;
            margin: 20px 0;
        }}
        .closing {{
            margin-top: 20px;
        }}
        .signature {{
            margin-top: 20px;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="greeting">
        <p>Hello {sender_name},</p>
    </div>
    
    <div class="success-box">
        <strong>Video Generated Successfully!</strong>
        <p>Your video has been created and is attached to this email.</p>
    </div>
    
    <div class="message">
        <p>Please find your generated video attached. The video was created using the images and script/prompt you provided.</p>
        
        <p>If you need any modifications or want to generate another video, feel free to start a new request or reply to this email.</p>
    </div>
    
    <div class="closing">
        <p>Thank you for using Video Companion!</p>
    </div>
    
    <div class="signature">
        <p>Best regards,<br>
        Video Companion Assistant</p>
    </div>
</body>
</html>
"""
        
        service = authenticate_gmail(GMAIL_CREDENTIALS)
        if service:
            send_email(
                service, 
                sender_email, 
                subject, 
                html_content, 
                thread_headers=headers,
                attachment_path=video_path,
                attachment_name=os.path.basename(video_path)
            )
            mark_message_as_read(service, message_id)
        
        logging.info("Sent video email with attachment")
    else:
        # Send error email
        html_content = f"""
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
        }}
        .greeting {{
            margin-bottom: 15px;
        }}
        .message {{
            margin: 15px 0;
        }}
        .error-box {{
            background-color: #f8d7da;
            border-left: 4px solid #dc3545;
            padding: 15px;
            margin: 20px 0;
        }}
        .closing {{
            margin-top: 20px;
        }}
        .signature {{
            margin-top: 20px;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="greeting">
        <p>Hello {sender_name},</p>
    </div>
    
    <div class="error-box">
        <strong>Video Generation Issue</strong>
        <p>We encountered an issue while generating your video. Our technical team has been notified.</p>
    </div>
    
    <div class="message">
        <p>We apologize for the inconvenience. Please try again later, or contact support if the issue persists.</p>
    </div>
    
    <div class="closing">
        <p>We'll work to resolve this as soon as possible.</p>
    </div>
    
    <div class="signature">
        <p>Best regards,<br>
        Video Companion Assistant</p>
    </div>
</body>
</html>
"""
        
        service = authenticate_gmail()
        if service:
            send_email(service, sender_email, subject, html_content, thread_headers=headers)
            mark_message_as_read(service, message_id)
        
        logging.info("Sent video generation error email")
# Main DAG definition
dag = DAG(
    'video_generation_and_merge_pipeline',
    default_args=default_args,
    description='Complete pipeline: Generate multiple videos and merge them',
    schedule_interval=None,
    catchup=False,
    tags=['video', 'generation', 'merge', 'pipeline'],
)

# Define all tasks
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

# Dynamic mapping - creates N parallel video generation tasks
process_segments = PythonOperator.partial(
    task_id='process_segment',
    python_callable=process_single_segment,
    dag=dag,
    pool="video_processing_pool"
).expand(op_kwargs=task3.output)

# Collect all videos in correct order
collect_task = PythonOperator(
    task_id='collect_videos',
    python_callable=collect_and_merge_videos,
    dag=dag,
)

# Final merge task
merge_task = PythonOperator(
    task_id='merge_all_videos',
    python_callable=merge_videos_wrapper,
    dag=dag,
)
# Final merge task
send_video_email = PythonOperator(
    task_id='send_video_email',
    python_callable=send_video_email,
    dag=dag,
)

# Set dependencies - this is the key part!
# task1 >> task2 >> task3 >> process_segments >> collect_task >> merge_task
task1 >> task2 >> task3 >> process_segments >> collect_task >> merge_task >> send_video_email