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
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import re
from google import genai
from google.genai import types
import random
from pathlib import Path
from airflow.exceptions import AirflowException
from datetime import date
from PIL import Image

from minio import Minio
from minio.error import S3Error


# Add the parent directory to Python path
import sys
dag_dir = Path(__file__).parent
sys.path.insert(0, str(dag_dir))
from agent.veo import GoogleVeoVideoTool


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "video_companion_developers",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retry_delay": timedelta(seconds=15),
    "retries": 1
}

VIDEO_COMPANION_FROM_ADDRESS = Variable.get("CF.companion.from.address")
GMAIL_CREDENTIALS = Variable.get("CF.companion.gmail.credentials")
SHARED_ROOT = "/appz/shared" 
CACHE_ROOT = "/appz/cache"
LOGS_ROOT = "/appz/logs"
GEMINI_API_KEY = Variable.get("CF.companion.gemini.api_key")
# --- ADD/UPDATE CONFIGURATION ---
# MinIO Configuration (Add these vars to your Airflow Admin -> Variables)
MINIO_ENDPOINT = Variable.get("CF_MINIO_ENDPOINT", default_var="minio:9000")
MINIO_ACCESS_KEY = Variable.get("CF_MINIO_USER", default_var="minioadmin")
MINIO_SECRET_KEY = Variable.get("CF_MINIO_PASSWORD", default_var="minioadmin")
MINIO_BUCKET = Variable.get("CF_MINIO_BUCKET", default_var="clipfoundry")
MINIO_SECURE = Variable.get("MINIO_SECURE", default_var="False").lower() == "true"

# Use the appropriate Gemini model (e.g. gemini-1.5-flash or gemini-1.5-pro)
GEMINI_MODEL = Variable.get("CF.companion.gemini.model", default_var="gemini-2.5-flash")
SCRIPT_GENERATION_TEMPLATE = """
# Your Role
Lead Scriptwriter & Researcher — Conduct background research on the provided topic and draft a cohesive, engaging narrative script optimized for short-form video content.

# Your Task
Research the user's idea and generate a single, continuous narrative script.

# Core Information
• Topic: "{idea}"
• Target Duration: {duration} seconds
• Tone: {tone}
• Max Word Count: {max_words} words (Strict Limit)

# Research & Strategy
• Analyze the Topic to identify key facts, themes, or plot points.
• If the topic is factual (e.g., History, Science), ensure accuracy based on your knowledge base.
• If the topic is abstract/creative, structure a logical story arc (Beginning, Middle, End).

# Script Guidelines
• Pacing: Write for a speaking rate of ~150 words per minute.
• Structure: The script must be written as a continuous flow.
• Formatting: Do not include visual directions, camera angles, or emojis in the spoken text. Only provide the spoken narrative (Voiceover).
• Length Control: Ensure the word count strictly aligns with the Max Word Count.

# Output Format (CRITICAL)
You must return a valid JSON object. Do not return markdown code blocks.
``json
    {{
        "video_meta": {{ 
            "title": "A short catchy title", 
            "video_type": "Reel/Short", 
            "target_duration": {duration}
        }},
        "script_content": "The single block of clear, engaging spoken text...",
        "visual_direction": "A brief summary of the visual style/mood for the video creator (separate from the script)."
    }}
    ```
"""

CLARITY_ANALYZER_SYSTEM = """
You are an expert Video Production QA Assistant.
Your only job is to strictly evaluate whether a user request contains enough information to generate a high-quality talking-head AI avatar video.

Rules:
- "has_clear_idea" = true only if the user clearly states the video type and goal.
- "request_type": 
    - "approval": If the user is replying to a previous script draft with "Approved", "Looks good", "Yes", "Go ahead", "Proceed", or "Make it".
    - "general_query": If the user is saying "Hello", "How are you", "Help", or chatting casually without video intent.
    - "video_request": If the user wants to make a NEW video or modify an existing idea.
- "target_duration": Extract the requested duration in seconds (integer) if mentioned (e.g., "30s", "1 minute" -> 60). If not mentioned, return null.
- "wpm_override": Detect if the user specified a speaking pace (e.g., "fast", "slow", "180 wpm"). Convert vague terms: "Fast"->190, "Slow"->130, "Normal"->150. If explicit number given, use it. Return null if not mentioned.
- Always output valid JSON only.

Required JSON format:
{
  "request_type": "video_request" | "general_query" | "approval",
  "has_clear_idea": true|false,
  "has_script": true|false,
  "idea_description": "summary",
  "suggested_title": "title",
  "tone": "tone",
  "action": "generate_video" | "generate_script",
  "aspect_ratio": "16:9" | "9:16", 
  "resolution": "720p",
  "wpm_override": 180,  // Integer or null
  "target_duration": 30
}
"""

CLIPFOUNDRY_CATALOGUE = """
# 🎬 ClipFoundry — Video Generation Agent  
### Your No-Code Creative Production Assistant

ClipFoundry is an AI-powered video generation agent designed for creative studios, marketers, and content teams. You can send an email or chat message, attach a model photo and a background image, provide a script or simple instructions—and receive a ready-to-use stitched video, all without touching editing tools.

---

## 🌟 What ClipFoundry Can Do

- **Turn ideas or scripts into videos**  
  Send a script or just a high-level brief. ClipFoundry will write a clean, time-aligned script if needed.

- **Generate short videos featuring your model**  
  Using a model photo + background image, ClipFoundry produces talking-head or branded creative videos.

- **Auto-edit and stitch scenes**  
  Each scene is generated, timed, rendered, and merged into one seamless MP4.

- **No-code workflow**  
  Just send an email or message. No timeline editing. No tools. No configurations.

- **Privacy-first execution**  
  Everything runs inside an isolated appliance—no external cloud dependency.

---

## 🧩 How ClipFoundry Works Internally

### 1. Input Collection  
You provide:
- A model photo  
- A background image  
- Either a script (`SCRIPT:`) or high-level instructions (`INSTRUCTION:`)

### 2. Agent Intelligence  
The agent:
- Determines whether to use your script or generate one  
- Breaks the script into time-bound scenes  
- Selects appropriate visuals  
- Chooses durations and pacing for each segment

### 3. Airflow-Powered Video Production  
Airflow orchestrates:
- Script analysis  
- Scene generation  
- Video rendering  
- Stitching and exporting the final MP4  
- Sending you the download link  

Shared storage acts as the central content hub.

---

## 💼 Who It’s Built For

- Creative studios  
- Marketing teams  
- Social media teams  
- Advertising agencies  
- Enterprise brand teams  

Perfect for teams producing **frequent creative refreshes**, **daily assets**, or **rapid campaign variations**.

---

## 🎯 Why It’s Perfect for Lowtouch.ai

- Fully autonomous end-to-end agent workflow  
- Real example of distributed orchestration across connectors, Airflow, and private model execution  
- Demonstrates how enterprises can deploy custom digital workers inside secure infrastructure  
- High-visibility output that is easy to test, iterate, and showcase for hackathons  

---

## 🚀 Ready to Create?

Send:
- A model photo  
- A background image  
- A script or instruction  

And I’ll return your finished MP4 video.  
Just tell me what you want to create!

"""

def get_minio_client():
    """Initialize MinIO client on demand (inside tasks)."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    return client

def upload_file_to_minio(local_path, chat_id, filename):
    """Uploads a local file to MinIO."""
    client = get_minio_client()
    try:
        # Check bucket existence here, inside the task/worker
        if not client.bucket_exists(MINIO_BUCKET):
             client.make_bucket(MINIO_BUCKET)
             
        object_name = f"{chat_id}/{filename}"
        client.fput_object(MINIO_BUCKET, object_name, local_path)
        logging.info(f"Uploaded {local_path} to s3://{MINIO_BUCKET}/{object_name}")
        return object_name
    except Exception as e:
        logging.error(f"MinIO Upload failed: {e}")
        raise

def download_file_from_minio(object_name, local_path):
    """Downloads a file from MinIO."""
    client = get_minio_client()
    try:
        client.fget_object(MINIO_BUCKET, object_name, local_path)
        logging.info(f"Downloaded {object_name} to {local_path}")
        return local_path
    except Exception as e:
        logging.error(f"MinIO Download failed: {e}")
        raise

def is_agent_trigger(conf):
    """Returns True if triggered by Agent (or missing 'From' header), False if Email."""
    agent_headers = conf.get("agent_headers")
    email_from = conf.get("email_data", {}).get("headers", {}).get("From")
    return bool(agent_headers) or not bool(email_from)

def authenticate_gmail():
    """Authenticate Gmail API."""
    try:
        # 1. Determine if credentials are Dict or String
        if isinstance(GMAIL_CREDENTIALS, dict):
            creds_info = GMAIL_CREDENTIALS
        else:
            # Parse string (e.g. from Airflow Variable) into Dict
            creds_info = json.loads(GMAIL_CREDENTIALS)

        # 2. Pass the Dict directly (Do NOT json.load it again)
        creds = Credentials.from_authorized_user_info(creds_info)
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logging.info(f"Authenticated Gmail: {profile.get('emailAddress')}")
        return service
    except Exception as e:
        logging.error(f"Gmail authentication failed: {e}")
        return None

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import base64

def send_reply_to_thread(service, thread_id, message_id, recipient, subject, reply_html_body):
    """
    Send an HTML reply in the same Gmail thread.
    This ensures all messages stay in the same thread → same workspace.
    """
    try:
        # Create multipart message (plain + HTML)
        msg = MIMEMultipart("alternative")
        msg["To"] = recipient
        msg["From"] = VIDEO_COMPANION_FROM_ADDRESS
        msg["Subject"] = f"Re: {subject}" if not subject.lower().startswith("re:") else subject
        msg["In-Reply-To"] = message_id
        msg["References"] = message_id  # Helps threading

        # Plain text fallback
        plain_text = re.sub(r"<[^>]+>", "", reply_html_body)
        msg.attach(MIMEText(plain_text, "plain"))
        msg.attach(MIMEText(reply_html_body, "html"))

        # Encode and send with threadId
        raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        body = {
            "raw": raw_message,
            "threadId": thread_id  # ← THIS KEEPS IT IN THE SAME THREAD
        }

        sent = service.users().messages().send(userId="me", body=body).execute()
        logging.info(f"Reply sent successfully in thread {thread_id}, message ID: {sent['id']}")
        return sent

    except Exception as e:
        logging.error(f"Failed to send reply in thread {thread_id}: {e}", exc_info=True)
        raise

def send_acknowledgement_email_logic(context):
    """
    Internal helper to send an immediate acknowledgement email.
    Called directly inside validate_prompt_clarity.
    """
    ti = context['ti']
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}

    # 1. Skip if triggered via Chat Agent (API)
    if is_agent_trigger(conf):
        logging.info("Agent trigger detected: Skipping acknowledgement email.")
        return

    # 2. Extract Email Data
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input")
    thread_id = ti.xcom_pull(key="thread_id", task_ids="agent_input")
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")

    # 3. Parse Sender Name
    sender_name = "there"
    if "<" in sender_email:
        sender_name = sender_email.split("<")[0].strip()
        sender_email = sender_email.split("<")[1].replace(">", "").strip()
    else:
        name_match = re.search(r'^([^<]+)', sender_email)
        if name_match:
            sender_name = name_match.group(1).strip()

    subject = headers.get("Subject", "Video Generation Request")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"

    # 4. Construct HTML Content
    html_content = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; }}
        .greeting {{ margin-bottom: 15px; }}
        .info-box {{ background-color: #e2e3e5; border-left: 4px solid #383d41; padding: 15px; margin: 20px 0; }}
        .signature {{ margin-top: 20px; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="greeting">
        <p>Hello {sender_name},</p>
    </div>
    
    <div class="info-box">
        <strong>Request Received!</strong>
        <p>I have successfully received your video generation request. The creative process has started.</p>
    </div>
    
    <div class="message">
        <p>Your video is currently being scripted, generated, and stitched. This process typically takes <strong>15 to 30 minutes</strong> depending on complexity.</p>
        <p>I will send you another email with the final video attached as soon as it is ready. No further action is required from you at this time.</p>
    </div>
    
    <div class="signature">
        <p>Best regards,<br>
        Video Companion Assistant</p>
    </div>
</body>
</html>
"""

    # 5. Send Email
    if sender_email and "@" in sender_email:
        service = authenticate_gmail()
        if service:
            try:
                original_message_id = headers.get("Message-ID", "")
                send_reply_to_thread(
                    service=service,
                    thread_id=thread_id,
                    message_id=original_message_id,
                    recipient=sender_email,
                    subject=subject,
                    reply_html_body=html_content
                )
                logging.info(f"Acknowledgement email sent to {sender_email}")
            except Exception as e:
                logging.error(f"Failed to send acknowledgement email: {e}")

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
        client = genai.Client(api_key=GEMINI_API_KEY)
        
        contents_payload = []
        if conversation_history:
            for turn in conversation_history:
                contents_payload.append(types.Content(role="user", parts=[types.Part.from_text(text=turn["prompt"])]))
                contents_payload.append(types.Content(role="model", parts=[types.Part.from_text(text=turn["response"])]))
        
        contents_payload.append(types.Content(role="user", parts=[types.Part.from_text(text=prompt)]))

        response = client.models.generate_content(
            model=GEMINI_MODEL,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                response_mime_type="application/json",
                temperature=temperature
            ),
            contents=contents_payload
        )
        return response.text
    except Exception as e:
        logging.error(f"Gemini API error: {e}", exc_info=True)
        error_msg = f"AI request failed: {str(e)}"
        # Return a JSON string on error for consistency with JSON mime type
        return json.dumps({"error": error_msg})

def extract_json_from_text(text):
    """Extract JSON from text."""
    try:
        if not text: return None
        text = text.strip()
        
        # 1. Try Direct Parse (Fastest)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 2. Clean Markdown Code Blocks
        # Remove ```json ... ``` or just ``` ... ```
        pattern = r"```(?:json)?\s*(.*?)\s*```"
        match = re.search(pattern, text, re.DOTALL)
        if match:
            text = match.group(1).strip()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                pass # Continue if markdown content wasn't clean JSON

        # 3. Robust Brute Force (Find outermost brackets)
        # Finds the first '{' and the last '}'
        start = text.find('{')
        end = text.rfind('}')
        
        if start != -1 and end != -1 and end > start:
            json_str = text[start : end + 1]
            return json.loads(json_str)
            
        return None
    except Exception as e:
        logging.error(f"JSON extraction error: {e}")
        return None

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
def agent_input_task(**kwargs):
    """
    Extract and standardize input from both Email and Agent sources.
    Pushes all raw and normalized data to XCom for downstream tasks.
    """
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    ti = kwargs['ti']
    
    # 1. Extract raw config sections
    agent_headers = conf.get("agent_headers", {})
    chat_inputs = conf.get("chat_inputs", {})
    email_data = conf.get("email_data", {})
    # 2. Unified Extraction Strategy
    # Check Chat Inputs first (Agent), then fallback to Email Data
    prompt = chat_inputs.get("message", "") or email_data.get("content", "")
    prompt = prompt.strip()
    
    # Images might be in 'files' (Agent) or 'images' (Email/Direct)
    images = chat_inputs.get("files", []) or conf.get("images", [])
    
    logging.info(f"Thinking: I've received your request. I'm now analyzing the {len(images)} image(s) and your prompt to understand exactly what you need...")

    detected_aspect_ratio = "16:9" # Default fallback
    if images and len(images) > 0:
        first_image = images[0]
        img_path = first_image.get("path") if isinstance(first_image, dict) else first_image
        
        if img_path and os.path.exists(img_path):
            try:
                with Image.open(img_path) as img:
                    width, height = img.size
                    if height > width:
                        detected_aspect_ratio = "9:16"
                    else:
                        detected_aspect_ratio = "16:9"
                    
                    logging.info(f"Detected Image Ratio: {width}x{height} -> {detected_aspect_ratio}")
            except Exception as e:
                logging.warning(f"Failed to detect aspect ratio: {e}")
    
    ti.xcom_push(key="detected_aspect_ratio", value=detected_aspect_ratio)

    chat_history = conf.get("chat_history", [])
    thread_id = conf.get("thread_id", "")
    message_id = conf.get("message_id", "")
    
    # 3. Push Standardized Data to XCom
    ti.xcom_push(key="agent_headers", value=agent_headers)
    
    # Update email_data to ensure downstream tasks find the content
    # regardless of where it came from
    updated_email_data = email_data.copy()
    updated_email_data["content"] = prompt
    ti.xcom_push(key="email_data", value=updated_email_data)
    
    ti.xcom_push(key="images", value=images)
    ti.xcom_push(key="chat_history", value=chat_history)
    ti.xcom_push(key="thread_id", value=thread_id)
    ti.xcom_push(key="message_id", value=message_id)
    ti.xcom_push(key="prompt", value=prompt)
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    # Push user email if available from agent headers
    if agent_headers and "X-LTAI-User" in agent_headers:
        ti.xcom_push(
            key="ltai-user-email",
            value=agent_headers["X-LTAI-User"].strip().lower()
        )
    elif sender_email:
        # Handle format: "Name <email@domain.com>"
        if "<" in sender_email:
            sender_email = sender_email.split("<")[1].replace(">", "").strip().lower()
        else:
            sender_email = sender_email.strip().lower()
        ti.xcom_push(
            key="ltai-user-email",
            value=sender_email
        )
    else:
        logging.warning("No user email identified from inputs.")
    
    logging.info(f"Input extracted - Prompt length: {len(prompt)}, Images: {len(images)}")
    return "freemium_guard"


def freemium_guard_task(**kwargs):
    """
    Enforce free tier limits for non-internal users.
    Internal users (ecloudcontrol.com) bypass limits.
    """
    ti = kwargs['ti']
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Get user email from previous task
    email = (ti.xcom_pull(
        task_ids="agent_input",
        key="ltai-user-email"
    ) or "").strip().lower()
    
    if not email:
        logging.warning("No user email found, skipping freemium guard")
        return "end"
    
    # Check if internal user
    domain = email.split("@")[-1]
    if domain == "ecloudcontrol.com":
        logging.info(f"Internal user detected: no limits applied")
        ti.xcom_push(key="user_type",value="Internal")
        return "validate_input"
    
    ti.xcom_push(key="user_type",value="external")
    # Check free tier usage
    bucket_key = f"clipfoundry_free::{email}::{today}"
    current = int(ti.xcom_pull(key=bucket_key, include_prior_dates=True) or 0)
    
    USAGE_LIMIT = Variable.get("CF.video.external.limit",default_var=5)
    logging.info(f"User has used {current}/{USAGE_LIMIT} all free videos")
    if current >= USAGE_LIMIT:
        logging.error(f"Free tier limit reached ")
        raise AirflowException(
            f"Free tier limit reached for. "
            "You've used all 5 free videos. "
            "Please upgrade or try again."
        )
    
    # Increment usage counter
    ti.xcom_push(key=bucket_key, value=current + 1)
    logging.info(f"Incremented usage for to {current + 1}")
    
    return "validate_input"


def validate_input(**kwargs):
    """
    Validate that required elements (prompt and images) are present.
    Routes to appropriate next step based on validation result and source.
    """
    ti = kwargs['ti']
    
    # Pull standardized data from agent_input_task
    prompt = ti.xcom_pull(task_ids="agent_input", key="prompt") or ""
    images = ti.xcom_pull(task_ids="agent_input", key="images") or []
    agent_headers = ti.xcom_pull(task_ids="agent_input", key="agent_headers") or {}
    email_data = ti.xcom_pull(task_ids="agent_input", key="email_data") or {}
    
    logging.info(f"Validating input - Prompt length: {len(prompt)}, Images: {len(images)}")
    
    logging.info("Thinking: Checking if I have all the necessary ingredients (images and a text description) to start production...")
    # 4. Validation Logic
    missing = []
    if not prompt:
        missing.append("prompt/idea")
    
    if missing:
        logging.warning(f"Missing required elements: {missing}")
        ti.xcom_push(key="validation_status", value="missing_elements")
        ti.xcom_push(key="missing_elements", value=missing)
        
        # Check Source - is this from Agent or Email?
        is_agent = bool(agent_headers) or not bool(email_data.get("headers", {}).get("From"))
        
        if is_agent:
            logging.info("Agent Trigger + Missing Info -> Stopping DAG (Agent will read XCom)")
            return "end"  # Bypass email task
        else:
            return "send_missing_elements_email"
    
    logging.info("Input validation passed")
    ti.xcom_push(key="validation_status", value="valid")
    logging.info("Thinking: Everything looks good! I have the required inputs to proceed with the creative process.")
    return "validate_prompt_clarity"

def validate_prompt_clarity(**kwargs):
    ti = kwargs['ti']
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input")
    chat_history = ti.xcom_pull(key="chat_history", task_ids="agent_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input")
    prompt = email_data.get("content", "").strip()
    detected_aspect_ratio = ti.xcom_pull(key="detected_aspect_ratio", task_ids="agent_input") or "16:9"
    
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
    Analyze this user message to determine the next step.

    USER MESSAGE: "{prompt}"
    DETECTED IMAGE RATIO: {detected_aspect_ratio}

    CRITICAL ROUTING RULES:
    1. "has_script": true ONLY if the user provided the EXACT VERBATIM text to be spoken.
    2. "has_script": false if the user provided an IDEA, TOPIC, or asked YOU to write the script.
    3. "action": "generate_video" if has_script is true.
    4. "action": "generate_script" if has_script is false.
    5. **ASPECT RATIO RULE (CRITICAL):** - DEFAULT to "{detected_aspect_ratio}" (to match the source image).
       - ONLY override this if the user EXPLICITLY asks for a different format (e.g. "make it portrait", "9:16", "landscape").
       - If the user says nothing about ratio, return "{detected_aspect_ratio}".
    6. Check if the user specified a duration (e.g., "make it 20 seconds").
    7. "wpm_override": Detect if the user specified a speaking pace (e.g., "fast", "slow", "180 wpm"). Convert vague terms: "Fast"->190, "Slow"->130, "Normal"->150. If explicit number given, use it. Return null if not mentioned.
    Return JSON:
    ``json
    {{
      "request_type": "video_request" | "general_query",
      "has_clear_idea": true|false,
      "has_script": true|false,
      "idea_description": "summary",
      "suggested_title": "title",
      "tone": "tone",
      "action": "generate_video" | "generate_script",
      "aspect_ratio": "16:9" | "9:16", 
      "resolution": "720p",
      "wpm_override": 180,  // Integer or null
      "target_duration": 30
    }}
    ```
    """
    logging.info("Thinking: I'm analyzing your request to determine the video style, tone, and script requirements...")

    response = get_gemini_response(
        prompt=analysis_prompt,
        system_instruction=CLARITY_ANALYZER_SYSTEM,
        conversation_history=conversation_history_for_ai
    )
    logging.info(f"AI Response is :{response}")
    
    analysis = extract_json_from_text(response) or {}
    
    # New Gatekeeper Logic
    if analysis.get("request_type") == "general_query":
        logging.info("General Query detected. Routing to automated reply.")
        return "send_general_response"
    
    if analysis.get("request_type") == "approval":
        logging.info("✅ Approval detected. Recovering context and skipping Acknowledgement email.")
        
        # Pass analysis down in case there are specific overrides (like WPM)
        ti.xcom_push(key="prompt_analysis", value={
            "has_clear_idea": True,
            "idea_description": "User approved previous script",
            "aspect_ratio": detected_aspect_ratio, # Inherit from image
            "resolution": "720p",
            "wpm_override": analysis.get("wpm_override"),
            "target_duration": analysis.get("target_duration")
        })
        return "split_script"
    
    # If we are here, the user WANTS a video. Now we must ensure they provided images.
    images = ti.xcom_pull(task_ids="agent_input", key="images") or []
    
    if not images:
        logging.warning("Video Request detected, but NO IMAGES provided.")
        
        # Set missing elements so the email knows what to ask for
        ti.xcom_push(key="missing_elements", value=["image(s)"])
        
        # Determine routing (Agent vs Email) similar to validate_input
        agent_headers = ti.xcom_pull(task_ids="agent_input", key="agent_headers") or {}
        email_data = ti.xcom_pull(task_ids="agent_input", key="email_data") or {}
        is_agent = bool(agent_headers) or not bool(email_data.get("headers", {}).get("From"))
        
        if is_agent:
            return "end"
        else:
            return "send_missing_elements_email"
    
    # Always store analysis (even if partial)
    idea_description = analysis.get("idea_description", "A short professional talking-head video")
    has_script = analysis.get("has_script", False) # Default to False if missing
    wpm_override = analysis.get("wpm_override")
    
    ti.xcom_push(key="prompt_analysis", value={
        "has_clear_idea": True,  # We force this now
        "idea_description": idea_description,
        "script_quality": "none",
        "suggested_title": analysis.get("suggested_title", "Your Video"),
        "tone": analysis.get("tone", "professional"),
        "aspect_ratio": analysis.get("aspect_ratio", "16:9"),
        "resolution": analysis.get("resolution", "720p"),
        "wpm_override": wpm_override,
        "target_duration": analysis.get("target_duration")
    })

    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    if sender_email and "@" in sender_email:        
        try:
            logging.info("Attempting to send acknowledgement email...")
            send_acknowledgement_email_logic(kwargs)
        except Exception as e:
            logging.error(f"Error sending acknowledgement email: {e}")
    
    # Only skip to video if we explicitly have a script
    if has_script:
        logging.info("User provided a script. Routing to Video Generation.")
        ti.xcom_push(key="final_script", value=prompt)
        logging.info("Thinking: I see you've provided a specific script. I will skip the writing phase and produce the video exactly as you wrote it.")
        return "split_script"
    else:
        logging.info(f"No script detected (Idea: {idea_description}). Routing to Script Generation.")
        logging.info(f"Thinking: I understand your idea. I will now write a creative script for you that fits this concept.")
        return "generate_script"

def send_missing_elements_email(**kwargs):
    """Send email about missing elements."""
    ti = kwargs['ti']
    
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input")
    missing_elements = ti.xcom_pull(key="missing_elements", task_ids="validate_input") or []
    agent_headers = ti.xcom_pull(key="agent_headers", task_ids="agent_input") or {}
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")

    if not sender_email:
        sender_email = agent_headers.get("X-LTAI-User", "")

    sender_name = "there"
    name_match = re.search(r'^([^<]+)', sender_email)
    if name_match:
        sender_name = name_match.group(1).strip()
        if "<" in sender_email:
            sender_email = sender_email.split("<")[1].replace(">", "").strip()
    
    subject = headers.get("Subject", "Video Generation Request")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    
    if missing_elements and isinstance(missing_elements, list):
        missing_list = " and ".join(missing_elements)
    else:
        # Fallback text if list is empty/None to prevent crash
        missing_list = "the required images or prompt"
    
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
    
    # 3. Send Email (ONLY if we have a valid email address)
    if sender_email and "@" in sender_email:
        service = authenticate_gmail()
        if service:
            thread_id = ti.xcom_pull(key="thread_id", task_ids="agent_input")
            original_message_id = headers.get("Message-ID", "")

            send_reply_to_thread(
                service=service,
                thread_id=thread_id,
                message_id=original_message_id,
                recipient=sender_email,
                subject=subject,
                reply_html_body=html_content
            )
            
            # 4. Mark Read (ONLY if we have a valid Message ID)
            if message_id:
                mark_message_as_read(service, message_id)
            
            logging.info(f"Sent missing elements email to {sender_email}")
    else:
        logging.info("No valid recipient email found. Skipping email send.")

def send_general_response(**kwargs):
    """Replies to general conversation (Hello, Help, etc.) via Gemini, always in Markdown."""
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input")
    prompt = email_data.get("content", "").strip()

    system_instruction = f"""
You are the ClipFoundry Video Generation Agent.

You MUST ALWAYS respond in clean Markdown (no JSON, no HTML).

# Behavior Rules

1. **If the user sends a greeting** (e.g., “hi”, “hello”, “hey”):
   - Reply with a warm, friendly greeting in Markdown.
   - Briefly introduce yourself as ClipFoundry, a video generation assistant.
   - Invite them to send a model photo, background image, and script/instructions.

2. **If the user asks about capabilities**  
   (e.g., “what can you do?”, “capabilities”, “help”, “what is this?”, “how does this work?”, “explain ClipFoundry”):
   - Respond ONLY with the following Markdown catalogue:
   
{CLIPFOUNDRY_CATALOGUE}

3. **For all other general inquiries**:
   - Provide a helpful Markdown explanation of how ClipFoundry works and how to start.

Your final output MUST be raw Markdown, with no JSON or backticks.
"""


    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        reply = client.models.generate_content(
            model=GEMINI_MODEL,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.4
            ),
            contents=[types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
        )
        reply_text = reply.text.strip()
    except Exception as e:
        reply_text = "Sorry, I could not process your request."

    # Remove accidental quotes
    if reply_text.startswith('"') and reply_text.endswith('"'):
        reply_text = reply_text[1:-1]

    # ---- Push agent response ----
    response_data = {
        "status": "success",
        "markdown_output": reply_text,
        "type": "text_reply"
    }
    ti.xcom_push(key="generated_output", value=response_data)

    # 3. Send Email (if applicable)
    if not is_agent_trigger(conf):
        headers = email_data.get("headers", {})
        sender_email = headers.get("From", "")
        if sender_email:
            service = authenticate_gmail()
            if service:
                html_body = reply_text.replace("\n", "<br>")
                send_reply_to_thread(
                    service=service,
                    thread_id=ti.xcom_pull(key="thread_id", task_ids="agent_input"),
                    message_id=headers.get("Message-ID", ""),
                    recipient=sender_email,
                    subject=headers.get("Subject", "Re: Video Companion"),
                    reply_html_body=f"<html><body>{html_body}</body></html>"
                )
    return response_data

def generate_script(**kwargs):
    """Generate video script based on user's idea."""
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input")
    chat_history = ti.xcom_pull(key="chat_history", task_ids="agent_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input")
    prompt_analysis = ti.xcom_pull(key="prompt_analysis", task_ids="validate_prompt_clarity")
    default_wpm = int(Variable.get("CF.script.wpm", default_var=170))
    user_wpm_override = prompt_analysis.get("wpm_override")
    AVG_WPM = int(user_wpm_override) if user_wpm_override else default_wpm
    
    logging.info(f"Using WPM: {AVG_WPM} (Source: {'User' if user_wpm_override else 'Variable'})")
    
    prompt = email_data.get("content", "").strip()
    idea_description = prompt_analysis.get("idea_description", "")
    target_tone = prompt_analysis.get("tone", "engaging and professional")
    requested_duration = prompt_analysis.get("target_duration")
    # Ensure it's a valid integer, otherwise default to 45
    if requested_duration and isinstance(requested_duration, int) and requested_duration > 5:
        TARGET_DURATION = requested_duration
        logging.info(f"Using user-requested duration: {TARGET_DURATION}s")
    else:
        TARGET_DURATION = 45
        logging.info(f"No valid user duration found. Using default: {TARGET_DURATION}s")

    # Build conversation history
    conversation_history_for_ai = []
    if chat_history:
        for i in range(0, len(chat_history), 2):
            if i + 1 < len(chat_history):
                user_msg = chat_history[i]
                assistant_msg = chat_history[i + 1]
                if user_msg["role"] == "user" and assistant_msg["role"] == "assistant":
                    conversation_history_for_ai.append({
                        "prompt": user_msg["content"],
                        "response": assistant_msg["content"]
                    })

    # 3. Retry Logic
    try_number = ti.try_number
    logging.info(f"Generating Script - Attempt #{try_number}")
    logging.info(f"Thinking: Drafting a compelling script for your video (Attempt #{try_number}). Ensuring it fits within the time limit...")
    
    constraint_note = ""
    if try_number > 1:
        history = ti.xcom_pull(key='rejected_script_history') or []
        reason = history[-1].get('reason') if history else "Unknown"
        logging.info(f"Retry Reason: {reason}")
        constraint_note = "CRITICAL: Previous script was TOO LONG. Write a significantly shorter version."

    # 4. Prompting
    MAX_WORDS = int((TARGET_DURATION / 60.0) * AVG_WPM)
    final_system_prompt = SCRIPT_GENERATION_TEMPLATE.format(
        idea=prompt,
        duration=TARGET_DURATION,
        tone=target_tone,
        max_words=MAX_WORDS
    )
    if constraint_note:
        final_system_prompt += f"\n\n**CRITICAL CORRECTION:** {constraint_note}"
    
    user_prompt = f"IDEA: {prompt}\nSUMMARY: {idea_description}\nGenerate script."

    # 5. Generate & Parse
    response_text = get_gemini_response(user_prompt, final_system_prompt, conversation_history_for_ai)
    data = extract_json_from_text(response_text)
    
    if not data or "script_content" not in data:
        logging.error(f"Invalid JSON: {response_text}")
        raise ValueError("Script generation failed: Invalid JSON")

    # 6. Validate
    script = data.get('script_content', '')
    est_time = (len(script.split()) / AVG_WPM) * 60
    logging.info(f"Duration: {est_time:.1f}s")
    
    if est_time > TARGET_DURATION * 1.2:
        reason = f"Duration {est_time:.1f}s too long"
        logging.warning(f"FAILING: {reason}")
        # Save History
        rej = {"attempt": try_number, "duration": est_time, "script": script, "reason": reason}
        old_hist = ti.xcom_pull(key='rejected_script_history') or []
        old_hist.append(rej)
        ti.xcom_push(key='rejected_script_history', value=old_hist)
        # Trigger Retry
        raise ValueError(reason)

    # 7. Format Output
    data['video_meta']['actual_duration'] = est_time
    meta = data.get('video_meta', {})
    script_html = script.replace("\n", "<br>")
    
    agent_markdown = f"""### 🎬 Script: {meta.get('title')}
**Target:** {TARGET_DURATION}s | **Est:** {est_time:.1f}s | **Type:** {meta.get('video_type')}
---
**📝 Script:**
> {script}

**🎨 Visuals:**
_{data.get('visual_direction')}_
"""

    email_html = f"""
    <div style="font-family: Arial; color: #333;">
        <h2 style="color: #2c3e50;">🎬 Script: {meta.get('title')}</h2>
        <p><b>Target:</b> {TARGET_DURATION}s | <b>Est:</b> {est_time:.1f}s</p>
        <h3 style="color: #2980b9;">📝 Script</h3>
        <blockquote style="background: #f9f9f9; border-left: 5px solid #2980b9; padding: 10px;">{script_html}</blockquote>
        <h3 style="color: #27ae60;">🎨 Visuals</h3>
        <div style="background: #eafaf1; padding: 10px;">{data.get('visual_direction')}</div>
    </div>
    """

    # 8. Send Email (Hybrid Check)
    if not is_agent_trigger(conf):
        headers = email_data.get("headers", {})
        sender_email = headers.get("From", "")
        
        if sender_email:
            logging.info("Sending approval email...")
            sender_name = "there"
            if "<" in sender_email:
                 sender_name = sender_email.split("<")[0].strip()
                 sender_email_addr = sender_email.split("<")[1].replace(">", "").strip()
            else:
                 sender_email_addr = sender_email

            subject = headers.get("Subject", "Video Script Approval")
            if not subject.lower().startswith("re:"): subject = f"Re: {subject}"

            service = authenticate_gmail()
            if service:
                body = f"<html><body><p>Hello {sender_name},</p><p>Script draft:</p>{email_html}</body></html>"
                thread_id = ti.xcom_pull(key="thread_id", task_ids="agent_input")
                original_message_id = headers.get("Message-ID", "")

                send_reply_to_thread(
                    service=service,
                    thread_id=thread_id,
                    message_id=original_message_id,
                    recipient=sender_email,
                    subject=subject,
                    reply_html_body=body
                )
                mark_message_as_read(service, message_id)
                logging.info(f"Script approval email sent to {sender_email_addr}")
    else:
        logging.info("Agent trigger detected: Skipping script approval email.")

    # 9. Return Unified Payload
    ti.xcom_push(key="generated_script", value=script)
    generated_output = {
        "status": "success",
        "markdown_output": agent_markdown,
        "email_html": email_html, 
        "raw_data": data
    }
    ti.xcom_push(key="generated_output", value=generated_output)
    logging.info(f"Thinking: Script drafted! It's estimated to be around {est_time:.0f} seconds long. Moving on to visual planning.")
    if not is_agent_trigger(conf):
        return 
    else:
        return 'split_script'


import math

def build_scene_config(segments_data, aspect_ratio="16:9", images=[], max_video_duration=90):
    """
    Transforms AI output into downstream config.
    NOW: Uses the 'duration' provided by the AI, with safety clamping.
    MAX_SCENES is calculated based on max_video_duration.
    Images are cycled through sequentially instead of random selection.
    """
    # Calculate MAX_SCENES based on video duration
    # Using 8 seconds as the average scene duration for calculation
    AVERAGE_SCENE_DURATION = 6.0
    MAX_SCENES = math.ceil(max_video_duration / AVERAGE_SCENE_DURATION)
    
    # Ensure at least 1 scene
    MAX_SCENES = max(1, MAX_SCENES)
    
    # Safety Limits (In case AI hallucinates a 20s or 1s duration)
    VALID_VEO_DURATIONS = [4, 6, 8]
    TARGET_WPM = 170.0
    
    final_config = []
    image_list = images
    
    # 1. Truncate
    if len(segments_data) > MAX_SCENES:
        segments_data = segments_data[:MAX_SCENES]

    for idx, item in enumerate(segments_data):
        # Extract fields
        text = item.get("text", "").strip()
        
        # Clean text
        text = re.sub(r'^(Narrator|Speaker|Scene \d+):?\s*', '', text, flags=re.IGNORECASE).strip()
        word_count = len(text.split())
        calculated_duration = (word_count / TARGET_WPM) * 60.0
        
        # Find the smallest valid duration that fits the text.
        # If text is longer than 8s, clamp to 8s (Veo max).
        final_duration = 8 
        for d in sorted(VALID_VEO_DURATIONS):
            if d >= (calculated_duration * 1.1):
                final_duration = d
                break
        
        # Safety buffer: If calculated is very close to the limit (e.g. 3.9s), bump to next tier
        # to account for pauses/breath.
        if final_duration == 4 and calculated_duration > 3.5:
            final_duration = 6
        elif final_duration == 6 and calculated_duration > 5.5:
            final_duration = 8
        
        # Cycle through images sequentially using modulo
        selected_image = None
        image_path = None
        
        if image_list:
            # Use modulo to rotate through images
            selected_image = image_list[idx % len(image_list)]
            image_path = selected_image.get("path")
        
        logging.info(f"Scene {idx}: Selected image path: {image_path}")

        # Extract the new fields we added to the prompt
        environment = item.get("environment", "Professional cinematic background")
        emotion = item.get("emotion", "Neutral professional")
        
        config_item = {
            "image_path": image_path,
            "prompt": text,
            "duration": int(round(final_duration)),
            "aspect_ratio": aspect_ratio,
            "environment": environment,
            "emotion": emotion
        }
        final_config.append(config_item)

    return final_config


# def get_config(**context):
#     """Task 1: Retrieve params and push to XCom."""
#     ti = context['ti']
    
#     dag_run = context.get('dag_run')
#     conf = dag_run.conf if dag_run else {}
#     config = {
#         'images': conf.get("images", {}),
#         'video_meta': {
#             "aspect_ratio": conf.get("aspect_ratio", "16:9"),
#             "resolution": conf.get("resolution", "720p"),
#         },
#         "script_content": conf.get("script_content", ""),
#     }
#     ti.xcom_push(key='config', value=config)
#     images_list = conf.get("images", {})
#     ti.xcom_push(key='images', value=images_list)
    
#     logging.info(f"Pushed config to XCom: {config}")

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
    You are a Video Pacing & Direction Expert for a TALKING HEAD video.
    
    TASK: Convert script lines into video production segments for a single speaker.
    
    CRITICAL RULES:
    1. **DURATION**: Assign 4s, 6s or 8s (no fractions).
    2. **CONTEXT**: For each segment, describe the **background environment** for the speaker.
       - **CONSTRAINT**: The video is a continuous shot of the speaker.
       - **DO NOT** describe montages, split-screens, text overlays, infographics, or visual metaphors.
       - **DO NOT** describe B-roll (e.g., "cars driving", "people walking").
       - **KEEP IT CONSISTENT**: For the 'environment' field, you MUST output exactly this string: "The exact background from the reference image. Static and unchanged."
       - **EMOTION**: Describe the speaker's facial expression and delivery tone.
    
    OUTPUT FORMAT: JSON List of Objects
    [
      { 
        "text": "Until now, a strange ancient anomaly waits beneath.", 
        "duration": 6,
        "environment": "Dimly lit server room background, depth of field",
        "emotion": "Mysterious, intense, whispering"
      }
    ]
    """

    # --- INPUT HANDLING ---
    
    images = ti.xcom_pull(key='images', task_ids='agent_input') 
    # script_content = raw_data.get('script_content')
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input")
    chat_history = ti.xcom_pull(key="chat_history", task_ids="agent_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input")
    prompt_analysis = ti.xcom_pull(key="prompt_analysis", task_ids="validate_prompt_clarity")
    aspect_ratio = prompt_analysis.get("aspect_ratio")
    # This one needs fixing - it should handle both script and generated_script
    generate_script = ti.xcom_pull(key="generated_output", task_ids="generate_script", default=None)

    # This line is problematic - it tries to check if generate_script (which could be a dict) equals string content
    text = email_data.get("content", "").strip()
    logging.info(f"{generate_script}, {text}")
    
    user_type = ti.xcom_pull(key="user_type", task_ids="freemium_guard")
    if user_type == "internal":
        MAX_DURATION = int(Variable.get("CF.video.internal.max_duration", default_var=90))
    elif user_type == "external":
        MAX_DURATION = int(Variable.get("CF.video.external.max_duration", default_var=90))
    else:
        # Optional: Handle other cases with a default
        MAX_DURATION = 90

    logging.info(f"📥 Processing Script... (Aspect Ratio: {aspect_ratio})")
    # Get the script - either generated or user-provided
   

    # 1. First, check if a script was generated in *this current run* (Script Generation Task)
    final_script = ti.xcom_pull(key="generated_script", task_ids="generate_script")
    
    # 2. If not, look into Chat History (for the MVP "Reply to proceed" flow)
    if not final_script:
        chat_history = ti.xcom_pull(key="chat_history", task_ids="agent_input") or []
        
        # Iterate backwards to find the latest Assistant message
        for msg in reversed(chat_history):
            if msg.get("role") == "assistant":
                raw_content = msg.get("content", "")
                
                # MVP CLEANUP: 
                # We simply split the string to get text between "📝 Script" and "🎨 Visuals"
                # This prevents the avatar from reading the metadata or visual instructions.
                if "📝 Script" in raw_content and "🎨 Visuals" in raw_content:
                    try:
                        # Split by Script header and take the second part
                        part_after_header = raw_content.split("📝 Script")[1]
                        # Split by Visuals header and take the first part
                        clean_script = part_after_header.split("🎨 Visuals")[0]
                        final_script = clean_script.strip()
                    except Exception as e:
                        logging.warning(f"Simple split failed, using raw content: {e}")
                        final_script = raw_content
                else:
                    # If headers are missing, fallback to the full message
                    final_script = raw_content
                
                logging.info(f"Recovered script from history (len={len(final_script)})")
                break

    # 3. Last Resort: User Input (Fallback if history is empty)
    if not final_script:
        final_script = email_data.get("content", "").strip()

    script_content = generate_script if generate_script is not None else final_script
    if not script_content or script_content == "Paste your full script here...":
        logging.error("❌ No script content provided.")
        return []
    if not script_content:
        logging.error("No script available for video generation")
        ti.xcom_push(key="video_generation_error", value="No script available")
        ti.xcom_push(key="video_generation_success", value=False)
        return
    
    # logging.info(f"Triggering video generation DAG for thread {thread_id}")
    
    logging.info("Thinking: I'm breaking down the script into individual scenes to ensure the video flows naturally...")
    # Step A: Clean Extraction
    draft_json = get_gemini_response(
        prompt=f"Extract spoken lines from:\n{script_content}",
        system_instruction=SYSTEM_PROMPT_EXTRACTOR,
        temperature=0.2,
        # api_key=GEMINI_API_KEY
    )
    
    try:
        logging.info(f"Initial Draft : {draft_json}")
        draft_data = extract_json_from_text(draft_json)
        draft_segments = draft_data.get("draft_segments", draft_data)
        if not isinstance(draft_segments, list):
            draft_segments = list(draft_data.values())[0] if isinstance(draft_data, dict) else []
    except:
        logging.error("Failed extraction step")
        return []

    if not draft_segments:
        logging.error("No script lines found")
        return []

    logging.info("Thinking: For each scene, I am calculating the perfect timing and assigning the best visual style to match the spoken words...")
    # Step B: Pacing
    final_json = get_gemini_response(
        prompt=f"Optimize these lines for natural video flow:\n{json.dumps(draft_segments)}",
        system_instruction=SYSTEM_PROMPT_FORMATTER,
        temperature=0.3, 
        # api_key=GEMINI_API_KEY
    )
    
    try:
        final_data = extract_json_from_text(final_json)
        final_segments = final_data.get("segments", final_data) if isinstance(final_data, dict) else final_data
        logging.info(f"Images: {images}")
        segments_for_processing = build_scene_config(
            segments_data=final_segments, 
            aspect_ratio=aspect_ratio, 
            images=images,
            max_video_duration=MAX_DURATION
        )
        
        ti.xcom_push(key="segments", value=segments_for_processing)
        return segments_for_processing

    except Exception as e:
        logging.error(f"Failed pacing step: {e}")
        return []

def process_single_segment(segment, segment_index, voice_persona, total_segments=1, **context):
    """
    Process ONE segment at a time.
    CRITICAL: segment_index preserves the ORDER of video generation.
    """
    ti = context['ti']
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    
    chat_id = conf.get("chat_inputs", {}).get("chat_id", "manual_run")
    chat_cache_dir = Path(CACHE_ROOT) / chat_id
    
    # Ensure it exists (redundancy check)
    chat_cache_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"Processing segment {segment_index}: {segment}")
    
    logging.info(f"Thinking: Animating Scene {segment_index + 1}... converting the text and image into a video segment.")
    video_model = Variable.get('CF.video.model', default_var='mock')

    base_instruction = "Maintain consistent eye contact. Keep head position stable in center frame."
    
    if total_segments > 1:
        if segment_index == 0:
            # FIRST CLIP: Hook the audience, then stabilize for the cut.
            continuity_instruction = (
                f"Part 1/{total_segments} (Intro). Start with an engaging smile and high energy. "
                f"{base_instruction} "
                "CRITICAL ENDING: Finish speaking, then hold a gentle smile and freeze posture for the last second."
            )
        elif segment_index == total_segments - 1:
            # LAST CLIP: Pick up smooth, end with a definitive sign-off.
            continuity_instruction = (
                f"Part {total_segments}/{total_segments} (Outro). Start speaking immediately. "
                f"{base_instruction} "
                "CRITICAL ENDING: Deliver the final line with authority, then hold a confident smile. Do not look away."
            )
        else:
            # MIDDLE CLIPS: The "Bridge". Must look identical to start/end of neighbors.
            continuity_instruction = (
                f"Part {segment_index + 1}/{total_segments} (Body). "
                f"{base_instruction} "
                "CRITICAL FLOW: Start speaking immediately. Do not tilt head or move hands wildly. "
                "At the end, hold the final pose perfectly still to allow for a seamless cut."
            )
    else:
        # SINGLE CLIP: Standard behavior
        continuity_instruction = "Standalone clip. Start engaging, deliver line, end with a smile."

    # --- FIX START: Handle S3 Image Paths ---
    image_path = segment.get('image_path')
    if image_path and image_path.startswith("s3://"):
        logging.info(f"Detected S3 image path: {image_path}. Downloading to local cache...")
        try:
            # Parse S3 URI to get the object name. 
            # Format: s3://bucket_name/object_name
            # We assume the bucket matches MINIO_BUCKET, so we just strip the prefix and bucket.
            bucket_name = Variable.get("CF_MINIO_BUCKET", default_var="clipfoundry")
            
            # Robust parsing
            path_no_scheme = image_path.replace("s3://", "")
            parts = path_no_scheme.split("/", 1) # Split into [bucket, key]
            
            if len(parts) == 2:
                # remote_bucket = parts[0] # Not used if we assume configured bucket
                object_name = parts[1]
                
                # Define local filename
                local_filename = f"seg_{segment_index}_{os.path.basename(object_name)}"
                local_dest_path = chat_cache_dir / local_filename
                
                # Download
                download_file_from_minio(object_name, str(local_dest_path))
                
                # Update image_path to the local path for the Veo tool
                image_path = str(local_dest_path)
                logging.info(f"Image downloaded to local path: {image_path}")
            else:
                logging.warning(f"Could not parse S3 path structure: {image_path}")
        except Exception as e:
            logging.error(f"Failed to download image from S3: {e}")
            raise
    # --- FIX END ---

    logging.info(f"Thinking: Animating Scene {segment_index + 1}... converting the text and image into a video segment.")
    video_model = Variable.get('CF.video.model', default_var='mock')
    
    video_path = None

    # --- 1. Generate Video (Mock or Veo) ---
    if video_model == 'mock':
        mock_list_raw = Variable.get('CF.mock_list', default_var='[]')
        mock_list = json.loads(mock_list_raw) if mock_list_raw else []
        path_index = segment_index % len(mock_list)
        video_path = mock_list[path_index]
        logging.info(f"Mock video {segment_index}: {video_path}")
        
        logging.info(f"Thinking: Scene {segment_index + 1} rendering complete.")
        # Return dict with index to preserve order
        return {
            'index': segment_index,
            'video_path': video_path
        }
    else:
        logging.info(f"Using Google Veo 3.0 for segment {segment_index}")
        try:
            veo_tool = GoogleVeoVideoTool(api_key=GEMINI_API_KEY)

            # Construct the scene context from the segment data
            # This combines Environment + Emotion for the "Scene" component
            env = segment.get('environment', '')
            emo = segment.get('emotion', '')
            scene_context_str = f"{env}. Atmosphere is {emo}"
            
            result = veo_tool._run(
                image_path=image_path, # <--- Use the updated local variable
                prompt=segment.get('prompt'),
                aspect_ratio=segment.get('aspect_ratio', '16:9'),
                duration_seconds=segment.get('duration', 6),
                output_dir=str(chat_cache_dir),
                continuity_context=continuity_instruction,
                voice_persona=voice_persona,
                scene_context=scene_context_str
                segment_index=segment_index,
                total_segments=total_segments
            )
            
            if result.get('success'):
                video_path = result['video_paths'][0]
                logging.info(f"✅ Generated segment {segment_index}: {video_path}")
            else:
                raise ValueError(f"Generation failed: {result.get('error')}")
        except Exception as e:
            logging.error(f"Exception in segment {segment_index}: {str(e)}")
            raise

    # --- 2. Upload to MinIO (Critical Update) ---
    if video_path and os.path.exists(video_path):
        try:
            filename = f"scene_{segment_index}_{os.path.basename(video_path)}"
            
            # Use helper function to upload
            minio_key = upload_file_to_minio(video_path, chat_id, filename)
            
            logging.info(f"Thinking: Scene {segment_index + 1} is successfully rendered and archived to storage.")
            logging.info(f"Scene {segment_index} moved to Object Storage: {minio_key}")
            
            # Optional: Remove local file to save space on worker node
            # os.remove(video_path) 

            return {
                'index': segment_index,
                'minio_key': minio_key,
                'status': 'success'
            }
        except Exception as e:
            logging.error(f"Failed to upload segment {segment_index} to MinIO: {e}")
            raise
    else:
        raise ValueError(f"Video generation failed, file not found: {video_path}")

def prepare_segments_for_expand(**context):
    """Convert segments list into format needed for expand with index tracking."""
    ti = context['ti']
    segments = ti.xcom_pull(task_ids='split_script', key='segments')
    images = ti.xcom_pull(task_ids='agent_input', key='images')
    
    client = genai.Client(api_key=GEMINI_API_KEY)
    if not segments:
        logging.warning("No segments found!")
        return []
    
    character_prompt = """
    Analyze the person in this image. Create a dense, static visual profile for a video generation AI.
    Include these 15+ attributes:
    1. Age & Gender
    2. Ethnicity & Skin Tone
    3. Hair (Color, Style, Length, Texture)
    4. Eyes (Color, Shape)
    5. Face Shape & Distinctive Features
    6. Body Build (Height, Weight, Type)
    7. Exact Clothing (Color, Material, Fit)
    8. Grooming & Accessories
    
    OUTPUT FORMAT:
    Return a single raw string describing the subject. No labels, just the description.
    Example:
    "A 30-year-old Japanese male with short spiked black hair, dark brown almond eyes, wearing a navy blue tailored suit jacket over a white crisp shirt, slim build, clean-shaven, serious professional expression."
    """
    
    if images and len(images) > 0:
        # We always use the FIRST image as the anchor for the voice identity
        ref_image_path = images[0].get("path") if isinstance(images[0], dict) else images[0]
        
        if ref_image_path and os.path.exists(ref_image_path):
            logging.info(f"Analyzing {ref_image_path} for visual character consistency...")
            try:
                with open(ref_image_path, "rb") as f:
                    img_bytes = f.read()
                response = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=[
                        types.Content(
                            role="user",
                            parts=[
                                types.Part.from_text(text=character_prompt),
                                types.Part.from_bytes(data=img_bytes, mime_type="image/jpeg")
                            ]
                        )
                    ]
                )
                voice_persona = response.text.strip()
                logging.info(f"✅ Established Voice Persona: {voice_persona}")
                
            except Exception as e:
                logging.error(f"Failed to generate voice persona: {e}")
    
    video_model = Variable.get('CF.video.model', default_var='mock')
    
    # Only limit segments if we are in Mock Mode to prevent loop crashes
    if video_model == 'mock':
        MOCK_LIMIT = 3
        if len(segments) > MOCK_LIMIT:
            logging.info(f"Mock Mode detected: Limiting segments from {len(segments)} to {MOCK_LIMIT} to prevent resource exhaustion.")
            segments = segments[:MOCK_LIMIT]
    else:
        logging.info(f"Production Mode ({video_model}): Processing all {len(segments)} segments.")
    
    total_segments = len(segments)
    
    # Return list of dicts with both segment and index for ordering
    return [
        {
            'segment': seg,
            'segment_index': idx,
            'total_segments': total_segments,
            'voice_persona': voice_persona
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
        logging.error("❌ No video segments were generated!")
        raise ValueError("No videos to merge")
    
    logging.info(f"📦 Collected {len(segment_results)} video results")
    
    # Sort by index to maintain correct order
    sorted_results = sorted(segment_results, key=lambda x: x['index'])
    # Extract MinIO Keys
    video_keys = [result['minio_key'] for result in sorted_results if 'minio_key' in result]
    
    if len(video_keys) < 1:
        raise ValueError("Insufficient videos for merge")

    req_id = context['dag_run'].run_id
    
    merge_params = {
        'video_keys': video_keys, # Renamed from video_paths
        'req_id': req_id
    }
    
    ti.xcom_push(key='merge_params', value=merge_params)
    logging.info(f"✅ Ready to merge {len(video_keys)} videos with req_id: {req_id}")
    logging.info(f"Thinking: All {len(video_keys)} scenes are rendered. I am collecting them now to prepare for the final edit.")
    return merge_params

def merge_videos_wrapper(**context):
    ti = context['ti']
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    chat_id = conf.get("chat_inputs", {}).get("chat_id", f"run_{dag_run.run_id}")
    
    merge_params = ti.xcom_pull(task_ids='collect_videos', key='merge_params')
    video_keys = merge_params.get('video_keys', [])

    # Create temp directory
    import shutil
    import uuid
    temp_dir = Path(CACHE_ROOT) / f"merge_{chat_id}_{uuid.uuid4()}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Download all segments
        local_video_paths = []
        for key in video_keys:
            filename = key.split('/')[-1]
            local_dest = temp_dir / filename
            download_file_from_minio(key, str(local_dest))
            local_video_paths.append(str(local_dest))
        
        # 2. Update params for local merger
        merge_params['video_paths'] = local_video_paths
        merge_params['work_dir'] = str(temp_dir)
        merge_params['output_dir'] = str(temp_dir)
        
        # 3. Run Merger
        from ffmpg_merger import merge_videos_logic
        modified_context = context.copy()
        modified_context['params'] = merge_params
        
        local_result_path = merge_videos_logic(**modified_context)
        
        # 4. Upload Final Video
        final_filename = f"final_{chat_id}.mp4"
        final_key = upload_file_to_minio(local_result_path, chat_id, final_filename)
        
        ti.xcom_push(key="generated_video_key", value=final_key)
        
        return {"status": "success", "minio_key": final_key}

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

def send_video(**kwargs):
    """Send generated video to user."""
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    video_key = ti.xcom_pull(key="generated_video_key", task_ids="merge_all_videos")
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input")
    thread_id = ti.xcom_pull(key="thread_id", task_ids="agent_input")
    video_path = ti.xcom_pull(key="generated_video_path", task_ids="merge_all_videos")
    import uuid
    video_path = Path(CACHE_ROOT) / f"attachment_{uuid.uuid4()}.mp4"
    download_file_from_minio(video_key, str(video_path))
    logging.info("Thinking: Sending the final generated video now.")
    # Check if this is an agent trigger - if so, just return the path
    if is_agent_trigger(conf):
        logging.info("Agent trigger detected: Skipping video email.")
        return {"video_path": video_path,"status":"success"}
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")
    
    # Extract sender name
    sender_name = "there"
    if "<" in sender_email:
        sender_name = sender_email.split("<")[0].strip()
        sender_email = sender_email.split("<")[1].replace(">", "").strip()
    else:
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
        
        service = authenticate_gmail()
        if service:
            try:
                # Create message with video attachment
                msg = MIMEMultipart()
                msg["To"] = sender_email
                msg["From"] = VIDEO_COMPANION_FROM_ADDRESS
                msg["Subject"] = subject
                
                # Add threading headers
                original_message_id = headers.get("Message-ID", "")
                if original_message_id:
                    msg["In-Reply-To"] = original_message_id
                    msg["References"] = original_message_id
                
                # Attach HTML body
                msg.attach(MIMEText(html_content, "html"))
                
                # Attach video file
                with open(video_path, "rb") as f:
                    video_attachment = MIMEBase("video", "mp4")
                    video_attachment.set_payload(f.read())
                    encoders.encode_base64(video_attachment)
                    video_attachment.add_header(
                        "Content-Disposition",
                        f"attachment; filename={os.path.basename(video_path)}"
                    )
                    msg.attach(video_attachment)
                
                # Send with thread ID
                raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode()
                body = {
                    "raw": raw_message,
                    "threadId": thread_id
                }
                
                sent = service.users().messages().send(userId="me", body=body).execute()
                logging.info(f"Video email sent successfully in thread {thread_id}, message ID: {sent['id']}")
                
                # Mark original message as read
                if message_id:
                    mark_message_as_read(service, message_id)
                
            except Exception as e:
                logging.error(f"Failed to send video email: {e}", exc_info=True)
                raise
    else:
        logging.error(f"Video file not found: {video_path}")
        # Optionally send an error email here
        raise FileNotFoundError(f"Generated video not found at {video_path}")
    
    logging.info(f"Video email sent to {sender_email}")
    
def send_error_email(**kwargs):
    """Send generic error email."""
    ti = kwargs['ti']
    
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input")
    
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
        thread_id = ti.xcom_pull(key="thread_id", task_ids="agent_input")
        original_message_id = headers.get("Message-ID", "")

        send_reply_to_thread(
            service=service,
            thread_id=thread_id,
            message_id=original_message_id,
            recipient=sender_email,
            subject=subject,
            reply_html_body=html_content
        )
        mark_message_as_read(service, message_id)
    
    logging.info("Sent generic error email")

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
    description=" creator:0.3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=3,
    doc_md=readme_content,
    tags=["video", "companion", "processor", "conversational"]
) as dag:

    agent_input_task = BranchPythonOperator(
        task_id="agent_input",
        python_callable=agent_input_task,
        provide_context=True,
        doc_md="Analyzing your request"
    )
    freemium_guard_task = BranchPythonOperator(
        task_id="freemium_guard",
        python_callable=freemium_guard_task,
        provide_context=True,
        doc_md="Checking usage limits"
    )
    validate_input_task = BranchPythonOperator(
        task_id="validate_input",
        python_callable=validate_input,
        provide_context=True,
        doc_md="Validating inputs"
    )

    validate_prompt_clarity_task = BranchPythonOperator(
        task_id="validate_prompt_clarity",
        python_callable=validate_prompt_clarity,
        provide_context=True,
        doc_md="Understanding your intent"
    )

    send_missing_elements_task = PythonOperator(
        task_id="send_missing_elements_email",
        python_callable=send_missing_elements_email,
        provide_context=True,
        doc_md="Requesting missing info"
    )

    # send_unclear_idea_task = PythonOperator(
    #     task_id="send_unclear_idea_email",
    #     python_callable=send_unclear_idea_email,
    #     provide_context=True
    # )

    generate_script_task = BranchPythonOperator(  # ✅ Correct for branching
        task_id="generate_script",
        python_callable=generate_script,
        provide_context=True,
        doc_md="Drafting video script"
    )



    send_error_email_task = PythonOperator(
        task_id="send_error_email",
        python_callable=send_error_email,
        provide_context=True,
        doc_md="Handling error"
    )

    send_general_response_task = PythonOperator(
        task_id="send_general_response",
        python_callable=send_general_response,
        provide_context=True,
        doc_md="Replying to query"
    )

    end_task = DummyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
        doc_md="Finished"
    )
    
    split_script = PythonOperator(
        task_id='split_script',
        python_callable=split_script_task,
        dag=dag,
        trigger_rule="none_failed_min_one_success",
        doc_md="Breaking down script"
    )

    prepare_segments = PythonOperator(
        task_id='prepare_segments',
        python_callable=prepare_segments_for_expand,
        dag=dag,
        doc_md="Preparing scenes"
    )

    # Dynamic mapping - creates N parallel video generation tasks
    process_segments = PythonOperator.partial(
        task_id='process_segment',
        python_callable=process_single_segment,
        dag=dag,
        pool="video_processing_pool",
        doc_md="Rendering scene",
        retries=3,
        retry_delay=timedelta(seconds=90)
    ).expand(op_kwargs=prepare_segments.output)

    # Collect all videos in correct order
    collect_task = PythonOperator(
        task_id='collect_videos',
        python_callable=collect_and_merge_videos,
        dag=dag,
        doc_md="Collecting scenes"
    )

    # Final merge task
    merge_task = PythonOperator(
        task_id='merge_all_videos',
        python_callable=merge_videos_wrapper,
        dag=dag,
        doc_md="Stitching final video"
    )
    # Final merge task
    send_video_task = PythonOperator(
        task_id='send_video',
        python_callable=send_video,
        dag=dag,
        doc_md="Delivering video"
    )

    # Set dependencies - this is the key part!
    # task1 >> task2 >> task3 >> process_segments >> collect_task >> merge_task
    
    # Task dependencies
    agent_input_task >> freemium_guard_task >> validate_input_task >> [validate_prompt_clarity_task, send_missing_elements_task]

    # From validate_prompt_clarity, branch to different paths
    validate_prompt_clarity_task >> [
        generate_script_task,
        split_script,  # Direct path when script provided
        send_error_email_task,
        send_general_response_task,
        send_missing_elements_task
    ]

    # Script generation path (leads to split_script OR end)
    generate_script_task >> split_script
    generate_script_task >> end_task

    # Video processing pipeline (starts from split_script)
    split_script >> prepare_segments >> process_segments >> collect_task >> merge_task >> send_video_task 

    # Error/completion paths
    send_missing_elements_task >> end_task
    send_error_email_task >> end_task