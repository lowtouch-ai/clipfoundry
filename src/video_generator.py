import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import logging
import shutil               # Added for file ops
import subprocess           # Added for FFmpeg
from pathlib import Path    # Added for path handling
from typing import List, Dict, Any # Added for typing
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import re
import google.generativeai as genai

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "video_companion_developers",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retry_delay": timedelta(seconds=15),
    "retries": 1
}

VIDEO_COMPANION_FROM_ADDRESS = Variable.get("video.companion.from.address")
GMAIL_CREDENTIALS = Variable.get("video.companion.gmail.credentials")
SHARED_IMAGES_DIR = "/appz/shared_images"
VIDEO_OUTPUT_DIR = "/appz/video_outputs"
GEMINI_API_KEY = Variable.get("video.companion.gemini.api_key")
genai.configure(api_key=GEMINI_API_KEY)

# Use the appropriate Gemini model (e.g. gemini-1.5-flash or gemini-1.5-pro)
GEMINI_MODEL = Variable.get("video.companion.gemini.model", default_var="gemini-2.5-flash")

# --- MOCK CONFIGURATION ---
MOCK_VIDEO_PARTS = [
    "/appz/home/airflow/dags/clipfoundry/src/mock_video/input/part_1.mp4",
    "/appz/home/airflow/dags/clipfoundry/src/mock_video/input/part_2.mp4",
    "/appz/home/airflow/dags/clipfoundry/src/mock_video/input/part_3.mp4"
]
TRANSITION_DURATION = 0.5

CLARITY_ANALYZER_SYSTEM = """
You are an expert Video Production QA Assistant.
Your only job is to strictly evaluate whether a user request contains enough information to generate a high-quality talking-head AI avatar video.

Rules:
- "has_clear_idea" = true only if the user clearly states the video type and goal (e.g., "product intro", "testimonial", "tutorial").
- "has_script" = true only if full spoken dialogue is provided.
- Always output valid JSON only. Never add explanations, markdown, or extra text.

Required JSON format (no deviations):
{
  "has_clear_idea": true|false,
  "has_script": true|false,
  "idea_description": "one-sentence summary or 'unclear'",
  "script_quality": "none|partial|complete",
  "reasoning": "short internal note"
}
"""

# ============================================================================
# NEW: FFmpeg Helper Functions
# ============================================================================

def get_video_metadata(file_path: str) -> Dict[str, Any]:
    """Uses ffprobe to extract width, height, and exact duration."""
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height,duration",
        "-of", "json",
        str(file_path)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        stream = data['streams'][0]
        return {
            'width': int(stream['width']),
            'height': int(stream['height']),
            'duration': float(stream.get('duration', 0))
        }
    except Exception as e:
        logger.error(f"Failed to probe {file_path}: {e}")
        raise

def prepare_segment(source_path: str, output_path: str, target_width: int, target_height: int) -> float:
    """Standardizes resolution and audio sample rate."""
    vf_filter = (
        f"scale={target_width}:{target_height}:force_original_aspect_ratio=decrease,"
        f"pad={target_width}:{target_height}:(ow-iw)/2:(oh-ih)/2"
    )
    
    cmd = [
        "ffmpeg", "-y",
        "-i", source_path,
        "-vf", vf_filter,
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
        "-r", "30",
        "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
        output_path
    ]
    
    logger.info(f"Standardizing segment: {source_path} -> {output_path}")
    subprocess.run(cmd, check=True, capture_output=True)
    return get_video_metadata(output_path)['duration']

def execute_merge_logic(video_paths: List[str], request_id: str, output_dir: str) -> str:
    """Executes the FFmpeg merge with transitions and fade-out fix."""
    
    # Setup Temp Workspace
    base_dir = Path(output_dir)
    temp_dir = base_dir / f"temp_{request_id}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    # 1. Determine Target Resolution from Clip 1
    meta_first = get_video_metadata(video_paths[0])
    target_w = meta_first['width']
    target_h = meta_first['height']
    
    # 2. Process Segments
    processed_clips = []
    clip_durations = []
    
    for idx, raw_clip in enumerate(video_paths):
        out_name = temp_dir / f"norm_{idx:03d}.mp4"
        duration = prepare_segment(str(raw_clip), str(out_name), target_w, target_h)
        processed_clips.append(str(out_name))
        clip_durations.append(duration)

    # 3. Build FFmpeg Filter
    inputs = []
    for clip in processed_clips:
        inputs.extend(["-i", clip])

    filter_parts = []
    cumulative_duration = clip_durations[0]
    
    current_v_label = "0:v"
    current_a_label = "0:a"
    
    for i in range(len(processed_clips) - 1):
        next_idx = i + 1
        
        offset = cumulative_duration - TRANSITION_DURATION
        if offset < 0: offset = 0
        
        is_last_transition = (i == len(processed_clips) - 2)
        target_v = "v_raw" if is_last_transition else f"v{i}"
        target_a = "a_raw" if is_last_transition else f"a{i}"
        
        filter_parts.append(
            f"[{current_v_label}][{next_idx}:v]"
            f"xfade=transition=fade:duration={TRANSITION_DURATION}:offset={offset}"
            f"[{target_v}]"
        )
        
        filter_parts.append(
            f"[{current_a_label}][{next_idx}:a]"
            f"acrossfade=d={TRANSITION_DURATION}:c1=tri:c2=tri"
            f"[{target_a}]"
        )
        
        current_v_label = target_v
        current_a_label = target_a
        cumulative_duration += (clip_durations[next_idx] - TRANSITION_DURATION)

    # 4. Final Fade Out (The Fix)
    FADE_OUT_DURATION = 1.0
    fade_start = cumulative_duration - FADE_OUT_DURATION
    if fade_start < 0: fade_start = 0
    
    filter_parts.append(f"[v_raw]fade=t=out:st={fade_start}:d={FADE_OUT_DURATION}[vout]")
    filter_parts.append(f"[a_raw]afade=t=out:st={fade_start}:d={FADE_OUT_DURATION}[aout]")

    full_filter = ";".join(filter_parts)

    # 5. Execute Merge
    final_filename = f"merged_{request_id}_{int(datetime.now().timestamp())}.mp4"
    final_path = base_dir / final_filename
    
    merge_cmd = [
        "ffmpeg", "-y",
        *inputs,
        "-filter_complex", full_filter,
        "-map", "[vout]", "-map", "[aout]",
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        "-shortest",
        str(final_path)
    ]
    
    logger.info(f"Running merge command: {' '.join(merge_cmd)}")
    subprocess.run(merge_cmd, capture_output=True, check=True)
    
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)
    return str(final_path)

# ============================================================================
# Existing Functions
# ============================================================================

def authenticate_gmail():
    """Authenticate Gmail API."""
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logging.info(f"Authenticated Gmail: {profile.get('emailAddress')}")
        return service
    except Exception as e:
        logging.error(f"Gmail authentication failed: {e}")
        return None

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

def extract_json_from_text(text):
    """Extract JSON from text."""
    try:
        text = text.strip()
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*', '', text)
        
        match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        return None
    except Exception as e:
        logging.error(f"JSON extraction error: {e}")
        return None

def send_email(service, recipient_email, subject, html_content, thread_headers=None, attachment_path=None, attachment_name=None):
    """Send email with optional attachment."""
    try:
        msg = MIMEMultipart()
        msg["From"] = f"Video Companion <{VIDEO_COMPANION_FROM_ADDRESS}>"
        msg["To"] = recipient_email
        msg["Subject"] = subject
        
        if thread_headers:
            original_message_id = thread_headers.get("Message-ID", "")
            if original_message_id:
                msg["In-Reply-To"] = original_message_id
                references = thread_headers.get("References", "")
                msg["References"] = f"{references} {original_message_id}".strip() if references else original_message_id
        
        msg.attach(MIMEText(html_content, "html"))
        
        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={attachment_name or os.path.basename(attachment_path)}")
            msg.attach(part)
            logging.info(f"Attached file: {attachment_name}")
        
        raw = base64.urlsafe_b64encode(msg.as_string().encode()).decode()
        result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
        
        logging.info(f"Email sent successfully: {result.get('id')}")
        return True
    except Exception as e:
        logging.error(f"Failed to send email: {e}", exc_info=True)
        return False

def mark_message_as_read(service, message_id):
    """Mark email as read."""
    try:
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'removeLabelIds': ['UNREAD']}
        ).execute()
        logging.info(f"Marked message {message_id} as read")
        return True
    except Exception as e:
        logging.error(f"Failed to mark as read: {e}")
        return False

def validate_input(**kwargs):
    """Validate if email has both prompt and images."""
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    email_data = conf.get("email_data", {})
    images = conf.get("images", [])
    chat_history = conf.get("chat_history", [])
    thread_id = conf.get("thread_id", "")
    message_id = conf.get("message_id", "")
    
    ti = kwargs['ti']
    ti.xcom_push(key="email_data", value=email_data)
    ti.xcom_push(key="images", value=images)
    ti.xcom_push(key="chat_history", value=chat_history)
    ti.xcom_push(key="thread_id", value=thread_id)
    ti.xcom_push(key="message_id", value=message_id)
    
    prompt = email_data.get("content", "").strip()
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    
    logging.info(f"Validating input - Prompt length: {len(prompt)}, Images: {len(images)}")
    
    # Check for missing elements
    missing = []
    if not prompt:
        missing.append("prompt/idea")
    if not images:
        missing.append("image(s)")
    
    if missing:
        logging.warning(f"Missing required elements: {', '.join(missing)}")
        ti.xcom_push(key="validation_status", value="missing_elements")
        ti.xcom_push(key="missing_elements", value=missing)
        return "send_missing_elements_email"
    
    logging.info("Input validation passed - has prompt and images")
    ti.xcom_push(key="validation_status", value="valid")
    return "validate_prompt_clarity"

def validate_prompt_clarity(**kwargs):
    ti = kwargs['ti']
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    chat_history = ti.xcom_pull(key="chat_history", task_ids="validate_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="validate_input")
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    prompt = email_data.get("content", "").strip()
    
    conversation_history_for_ai = []
    for i in range(0, len(chat_history), 2):
        if i + 1 < len(chat_history):
            user_msg = chat_history[i]
            assistant_msg = chat_history[i + 1]
            if user_msg["role"] == "user" and assistant_msg["role"] == "assistant":
                conversation_history_for_ai.append({
                    "prompt": user_msg["content"],
                    "response": assistant_msg["content"]
                })
    
    # Still do analysis to extract useful context (topic, tone, etc.)
    analysis_prompt = f"""
Analyze this user message and extract the best possible video idea:

USER MESSAGE: "{prompt}"

Return ONLY valid JSON:
{{
  "has_clear_idea": true|false,
  "idea_description": "best possible one-sentence summary of what video to make",
  "suggested_title": "short catchy title for the video",
  "target_audience": "who this video is for (guess if needed)",
  "tone": "professional|friendly|energetic|calm|motivational|etc",
  "reasoning": "how you interpreted the request",
  "action": "generate_video" | "generate_script",
}}
- If action is generate_video:
    - Parse the resolution and aspect ratio from USER MESSAGE. 
    - If the User doesnot specify any resolution or aspect ratio keep it to default as :
        aspect_ratio:16:9
        resoltuion:720p
    - add both values in above returning json.
"""

    response = get_gemini_response(
        prompt=analysis_prompt,
        system_instruction=CLARITY_ANALYZER_SYSTEM,
        conversation_history=conversation_history_for_ai
    )
    logging.info(f"AI Response is :{response}")
    
    try:
        analysis = extract_json_from_text(response) 
    except:
        analysis =  {}
        raise
    
    # Always store analysis (even if partial)
    idea_description = analysis.get("idea_description", "A short professional talking-head video")
    if analysis.get("has_clear_idea") == False:
        idea_description = f"Based on your message, I think you want: {idea_description}"
    else:
        idea_description = analysis.get("idea_description", idea_description)
    
    ti.xcom_push(key="prompt_analysis", value={
        "has_clear_idea": True,  # We force this now
        "idea_description": idea_description,
        "script_quality": "none",
        "suggested_title": analysis.get("suggested_title", "Your Video"),
        "tone": analysis.get("tone", "professional"),
        "aspect_ratio": analysis.get("aspect_ratio", "720p"),
        "resolution": analysis.get("resolution", "16:9")
    })
    
    logging.info(f"Forcing script generation with interpreted idea: {idea_description}")
    action = analysis.get("action")
    if action == "generate_video":
        # User already gave script or approved → go straight to video
        # We also store the raw user message as the final script
        ti.xcom_push(key="final_script", value=prompt)
        return "generate_video"
    else:
        return "generate_script"

def send_missing_elements_email(**kwargs):
    """Send email about missing elements."""
    ti = kwargs['ti']
    
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="validate_input")
    missing_elements = ti.xcom_pull(key="missing_elements", task_ids="validate_input")
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    sender_name = "there"
    name_match = re.search(r'^([^<]+)', sender_email)
    if name_match:
        sender_name = name_match.group(1).strip()
    
    subject = headers.get("Subject", "Video Generation Request")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    
    missing_list = " and ".join(missing_elements)
    
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
        .missing-items {{
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin: 20px 0;
        }}
        .requirements {{
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
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
    
    <div class="message">
        <p>Thank you for your video generation request. However, I need some additional information to proceed.</p>
    </div>
    
    <div class="missing-items">
        <strong>Missing Required Elements:</strong>
        <p>I couldn't find the following in your request: <strong>{missing_list}</strong></p>
    </div>
    
    <div class="requirements">
        <strong>What I Need:</strong>
        <ul>
            <li><strong>Image(s):</strong> One or more photos of the person from different angles (front, side, etc.)</li>
            <li><strong>Prompt/Idea:</strong> A clear description of what kind of video you want to create</li>
        </ul>
    </div>
    
    <div class="message">
        <p>Please reply to this email with the missing information, and I'll be happy to generate your video!</p>
    </div>
    
    <div class="closing">
        <p>Looking forward to hearing from you.</p>
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
    
    logging.info("Sent missing elements email")

def send_unclear_idea_email(**kwargs):
    """Send email about unclear idea."""
    ti = kwargs['ti']
    
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="validate_input")
    prompt_analysis = ti.xcom_pull(key="prompt_analysis", task_ids="validate_prompt_clarity")
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    sender_name = "there"
    name_match = re.search(r'^([^<]+)', sender_email)
    if name_match:
        sender_name = name_match.group(1).strip()
    
    subject = headers.get("Subject", "Video Generation Request")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    
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
        .issue-box {{
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin: 20px 0;
        }}
        .examples {{
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
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
    
    <div class="message">
        <p>Thank you for providing the images and your request. However, I need more clarity about what kind of video you'd like me to create.</p>
    </div>
    
    <div class="issue-box">
        <strong>Issue:</strong>
        <p>Your prompt doesn't clearly specify the type of video or the goal you want to achieve.</p>
    </div>
    
    <div class="examples">
        <strong>Examples of Clear Ideas:</strong>
        <ul>
            <li>"Create a professional talking head video where I introduce our new product"</li>
            <li>"Make a video of me explaining the benefits of our service as a testimonial"</li>
            <li>"Generate a video where I'm presenting the quarterly sales results"</li>
            <li>"Create a video tutorial where I explain how to use our software"</li>
        </ul>
    </div>
    
    <div class="message">
        <p>Please reply with a clearer description of:</p>
        <ul>
            <li>What type of video you want (talking head, presentation, tutorial, etc.)</li>
            <li>What the purpose or goal of the video is</li>
            <li>The context or setting you envision</li>
        </ul>
    </div>
    
    <div class="closing">
        <p>Once I understand your vision better, I can either generate a script for you or proceed with your existing content.</p>
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
    
    logging.info("Sent unclear idea email")

def generate_script(**kwargs):
    """Generate video script based on user's idea."""
    ti = kwargs['ti']
    
        
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    chat_history = ti.xcom_pull(key="chat_history", task_ids="validate_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="validate_input")
    prompt_analysis = ti.xcom_pull(key="prompt_analysis", task_ids="validate_prompt_clarity")
    
    prompt = email_data.get("content", "").strip()
    idea_description = prompt_analysis.get("idea_description", "")
    
    # Build conversation history
    conversation_history_for_ai = []
    for i in range(0, len(chat_history), 2):
        if i + 1 < len(chat_history):
            user_msg = chat_history[i]
            assistant_msg = chat_history[i + 1]
            if user_msg["role"] == "user" and assistant_msg["role"] == "assistant":
                conversation_history_for_ai.append({
                    "prompt": user_msg["content"],
                    "response": assistant_msg["content"]
                })
    
    script_prompt = f"""You are a professional video script writer. Create a detailed, engaging script for a video based on this idea:

USER'S IDEA: "{prompt}"
IDEA SUMMARY: "{idea_description}"

Generate a complete video script that includes:
1. Opening/Introduction (2-3 sentences)
2. Main content/body (4-6 sentences covering key points)
3. Closing/Call-to-action (1-2 sentences)

The script should be:
- Natural and conversational
- Appropriate for a talking head video format
- Between 30-60 seconds when spoken
- Clear and engaging

Return ONLY the script text, no additional formatting or explanations.
"""
    
    generated_script = get_gemini_response(
        prompt=script_prompt,
        conversation_history=conversation_history_for_ai
    )
    
    if not generated_script or "error" in generated_script.lower():
        logging.error("Script generation failed")
        ti.xcom_push(key="script_generation_error", value=True)
        return
    
    logging.info(f"Generated script (length: {len(generated_script)} chars)")
    ti.xcom_push(key="generated_script", value=generated_script)
    
    # Send confirmation email
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    sender_name = "there"
    name_match = re.search(r'^([^<]+)', sender_email)
    if name_match:
        sender_name = name_match.group(1).strip()
    
    subject = headers.get("Subject", "Video Generation Request")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    
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
        .script-box {{
            background-color: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 5px;
            padding: 20px;
            margin: 20px 0;
            white-space: pre-wrap;
            font-family: 'Courier New', monospace;
            font-size: 14px;
        }}
        .confirmation-request {{
            background-color: #d1ecf1;
            border-left: 4px solid #0c5460;
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
    
    <div class="message">
        <p>Great! I've analyzed your idea and generated a script for your video. Please review it below:</p>
    </div>
    
    <div class="script-box">
{generated_script}
    </div>
    
    <div class="confirmation-request">
        <strong>Next Steps:</strong>
        <p>Please review the script and let me know:</p>
        <ul>
            <li><strong>If you approve:</strong> Reply with "approved", "looks good", "proceed", or any confirmation</li>
            <li><strong>If you want changes:</strong> Reply with your requested modifications</li>
        </ul>
    </div>
    
    <div class="message">
        <p>Once you confirm, I'll proceed with generating your video using the provided images.</p>
    </div>
    
    <div class="closing">
        <p>Looking forward to your feedback!</p>
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
    
    logging.info("Sent script confirmation email")

# ============================================================================
# MODIFIED: generate_video (Outputs List of Files)
# ============================================================================

def generate_video(**kwargs):
    """
    Simulates generating video parts.
    Outputs a LIST of file paths to XCom for the merger task.
    """
    ti = kwargs['ti']
    thread_id = ti.xcom_pull(key="thread_id", task_ids="validate_input") or "mock_test"
    
    logging.info(f"Generating video parts for thread {thread_id}")
    
    # In Mock mode, we verify the 3 mock parts exist
    valid_parts = [p for p in MOCK_VIDEO_PARTS if os.path.exists(p)]
    
    if len(valid_parts) < 2:
        error_msg = f"Insufficient mock parts found. Needed 3, found {len(valid_parts)}."
        logging.error(error_msg)
        # We push success=False so downstream knows to fail (or handle error)
        ti.xcom_push(key="video_generation_success", value=False) 
        ti.xcom_push(key="video_generation_error", value=error_msg)
        return

    logging.info(f"Successfully located {len(valid_parts)} video parts.")
    
    # Push the LIST of parts to XCom
    ti.xcom_push(key="generated_video_parts", value=valid_parts)
    ti.xcom_push(key="video_generation_success", value=True)

# ============================================================================
# NEW TASK: merge_videos (Takes List -> Merges -> Outputs Single File)
# ============================================================================

def merge_videos(**kwargs):
    """
    Pulls list of videos from generate_video, merges them, and outputs final path.
    """
    ti = kwargs['ti']
    
    # Check if generation was successful
    gen_success = ti.xcom_pull(key="video_generation_success", task_ids="generate_video")
    if not gen_success:
        logging.warning("Skipping merge because generation failed.")
        ti.xcom_push(key="video_generation_success", value=False) # Propagate failure
        return

    # Pull the list of parts
    video_parts = ti.xcom_pull(key="generated_video_parts", task_ids="generate_video")
    thread_id = ti.xcom_pull(key="thread_id", task_ids="validate_input") or "mock_test"

    try:
        os.makedirs(VIDEO_OUTPUT_DIR, exist_ok=True)
        logging.info("Starting Merge Process...")
        
        merged_path = execute_merge_logic(
            video_paths=video_parts,
            request_id=thread_id,
            output_dir=VIDEO_OUTPUT_DIR
        )
        
        logging.info(f"Merge Complete: {merged_path}")
        
        # Push final single video path for email task
        ti.xcom_push(key="generated_video_path", value=merged_path)
        ti.xcom_push(key="video_generation_success", value=True)
        
    except Exception as e:
        logging.error(f"Merge failed: {e}", exc_info=True)
        ti.xcom_push(key="video_generation_success", value=False)
        ti.xcom_push(key="video_generation_error", value=str(e))

# ============================================================================
# MODIFIED: send_video_email (Reads from merge_videos task)
# ============================================================================

def send_video_email(**kwargs):
    """Send generated video to user."""
    ti = kwargs['ti']
    
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="validate_input")
    video_path = ti.xcom_pull(key="generated_video_path", task_ids="generate_video")
    video_success = ti.xcom_pull(key="video_generation_success", task_ids="generate_video")
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    sender_name = "there"
    name_match = re.search(r'^([^<]+)', sender_email)
    if name_match:
        sender_name = name_match.group(1).strip()
    
    subject = headers.get("Subject", "Video Generation Request")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    
    if video_success and video_path and os.path.exists(video_path):
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
        
        service = authenticate_gmail()
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

def send_error_email(**kwargs):
    """Send generic error email."""
    ti = kwargs['ti']
    
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="validate_input")
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    sender_name = "there"
    name_match = re.search(r'^([^<]+)', sender_email)
    if name_match:
        sender_name = name_match.group(1).strip()
    
    subject = headers.get("Subject", "Video Generation Request")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    
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
        <strong>Processing Error</strong>
        <p>We encountered an unexpected issue while processing your request.</p>
    </div>
    
    <div class="message">
        <p>Our technical team has been notified and will investigate the issue. Please try submitting your request again.</p>
        
        <p>If the problem persists, please contact our support team for assistance.</p>
    </div>
    
    <div class="closing">
        <p>We apologize for the inconvenience.</p>
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
    
    logging.info("Sent generic error email")

# ================= DAG DEFINITION =================

readme_content = """
# Video Companion Processor DAG

This DAG processes video generation requests from email.

## Workflow
1. Validate input (check for images and prompt)
2. Validate prompt clarity (check for clear idea and script)
3. Generate script if needed
4. Generate video
5. Send result to user

## Features
- Input validation with helpful error messages
- AI-powered script generation
- Thread-aware conversation history
- Professional email responses
"""

with DAG(
    "video_companion_processor",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["video", "companion", "processor"],
    params={
        "email_data": {
            # Providing a clear script ensures validation passes and jumps straight to generation
            "content": """
            Please create a video using the attached images. I have the script ready:
            
            "Welcome to Lowtouch AI. This is a test of the new video merging pipeline. 
            We are verifying that the three video clips merge seamlessly with cross-fade transitions 
            and that the audio levels are consistent. System status is nominal."
            """,
            "headers": {
                # A valid email format is required to avoid "Recipient address required" error
                "From": "manual_tester@lowtouch.ai", 
                "Subject": "Manual Test: Video Merge Pipeline",
                "Message-ID": "manual_test_id_001"
            }
        },
        "images": [
            # This dummy entry is REQUIRED so validate_input doesn't fail
            # The actual generation task will ignore this path and use your MOCK_VIDEO_PARTS list
            {"path": "/appz/shared_images/mock_placeholder.jpg"} 
        ],
        "chat_history": [],
        "thread_id": "manual_merge_test_run",
        "message_id": "manual_msg_001"
    }
) as dag:

    validate_input_task = BranchPythonOperator(
        task_id="validate_input",
        python_callable=validate_input,
        provide_context=True
    )

    validate_prompt_clarity_task = BranchPythonOperator(
        task_id="validate_prompt_clarity",
        python_callable=validate_prompt_clarity,
        provide_context=True
    )

    send_missing_elements_task = PythonOperator(
        task_id="send_missing_elements_email",
        python_callable=send_missing_elements_email,
        provide_context=True
    )

    send_unclear_idea_task = PythonOperator(
        task_id="send_unclear_idea_email",
        python_callable=send_unclear_idea_email,
        provide_context=True
    )

    generate_script_task = PythonOperator(
        task_id="generate_script",
        python_callable=generate_script,
        provide_context=True
    )

    generate_video_task = PythonOperator(
        task_id="generate_video",
        python_callable=generate_video,
        provide_context=True
    )

    # NEW TASK
    merge_videos_task = PythonOperator(
        task_id="merge_videos",
        python_callable=merge_videos,
        provide_context=True
    )

    send_video_email_task = PythonOperator(
        task_id="send_video_email",
        python_callable=send_video_email,
        provide_context=True
    )

    send_error_email_task = PythonOperator(
        task_id="send_error_email",
        python_callable=send_error_email,
        provide_context=True
    )

    end_task = DummyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    # Task dependencies
    validate_input_task >> [validate_prompt_clarity_task, send_missing_elements_task]
    
    validate_prompt_clarity_task >> [
        send_unclear_idea_task,
        generate_script_task,
        generate_video_task,
        send_error_email_task
    ]
    
    generate_script_task >> end_task
    
    # UPDATED PIPELINE: Generate -> Merge -> Email
    generate_video_task >> merge_videos_task >> send_video_email_task >> end_task
    
    send_missing_elements_task >> end_task
    send_unclear_idea_task >> end_task
    send_error_email_task >> end_task