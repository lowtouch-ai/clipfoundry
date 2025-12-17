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
SHARED_IMAGES_DIR = "/appz/shared_images"
VIDEO_OUTPUT_DIR = "/appz/video_outputs"
GEMINI_API_KEY = Variable.get("CF.companion.gemini.api_key")

# Use the appropriate Gemini model (e.g. gemini-1.5-flash or gemini-1.5-pro)
GEMINI_MODEL = Variable.get("CF.companion.gemini.model", default_var="gemini-2.5-flash")
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
    if sender_email:
        ti.xcom_push(
            key="ltai-user-email",
            value=sender_email.strip().lower()
        )
    if agent_headers and "X-LTAI-User" in agent_headers:
        ti.xcom_push(
            key="ltai-user-email",
            value=agent_headers["ltai-user-email"].strip().lower()
        )
    
    logging.info(f"Input extracted - Prompt length: {len(prompt)}, Images: {len(images)}")
    return "freemium_guard_task"


def freemium_guard_task(**kwargs):
    """
    Enforce free tier limits for non-internal users.
    Internal users (ecloudcontrol.com) bypass limits.
    """
    ti = kwargs['ti']
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Get user email from previous task
    email = (ti.xcom_pull(
        task_ids="agent_input_task",
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
    
    # Check free tier usage
    bucket_key = f"clipfoundry_free_daily::{email}::{today}"
    current = int(ti.xcom_pull(key=bucket_key, include_prior_dates=True) or 0)
    
    USAGE_LIMIT = Validate.get("CF.video.external.limit",default_var=5)
    logging.info(f"User has used {current}/{USAGE_LIMIT} free videos today")
    if current >= USAGE_LIMIT:
        logging.error(f"Free tier limit reached ")
        raise AirflowException(
            f"Free tier limit reached for. "
            "You've used all 5 free videos for today. "
            "Please upgrade or try again tomorrow."
        )
    
    # Increment usage counter
    ti.xcom_push(key=bucket_key, value=current + 1)
    logging.info(f"Incremented usage for to {current + 1}")
    ti.xcom_push(key="user_type",value="external")
    
    return "validate_input"


def validate_input(**kwargs):
    """
    Validate that required elements (prompt and images) are present.
    Routes to appropriate next step based on validation result and source.
    """
    ti = kwargs['ti']
    
    # Pull standardized data from agent_input_task
    prompt = ti.xcom_pull(task_ids="agent_input_task", key="prompt") or ""
    images = ti.xcom_pull(task_ids="agent_input_task", key="images") or []
    agent_headers = ti.xcom_pull(task_ids="agent_input_task", key="agent_headers") or {}
    email_data = ti.xcom_pull(task_ids="agent_input_task", key="email_data") or {}
    
    logging.info(f"Validating input - Prompt length: {len(prompt)}, Images: {len(images)}")
    
    # 4. Validation Logic
    missing = []
    if not prompt:
        missing.append("prompt/idea")
    
    # NOTE: If you want to allow Script Generation WITHOUT images, 
    # remove the next two lines. Currently, images are mandatory.
    if not images:
        missing.append("image(s)")
    
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
    return "validate_prompt_clarity"

def validate_prompt_clarity(**kwargs):
    ti = kwargs['ti']
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input_task")
    chat_history = ti.xcom_pull(key="chat_history", task_ids="agent_input_task")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input_task")
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
Analyze this user message to determine the next step.

USER MESSAGE: "{prompt}"

    CRITICAL ROUTING RULES:
    1. "has_script": true ONLY if the user provided the EXACT VERBATIM text to be spoken.
    2. "has_script": false if the user provided an IDEA, TOPIC, or asked YOU to write the script.
    3. "action": "generate_video" if has_script is true.
    4. "action": "generate_script" if has_script is false.
    5. Options for aspect_ratio are 9:16 and 16:9 give this based on user request
    Return JSON:
    ``json
    {{
      "has_clear_idea": true|false,
      "has_script": true|false,
      "idea_description": "summary",
      "suggested_title": "title",
      "tone": "tone",
      "action": "generate_video" | "generate_script",
      "aspect_ratio": "16:9", 
      "resolution": "720p"
    }}
    ```
    """

    response = get_gemini_response(
        prompt=analysis_prompt,
        system_instruction=CLARITY_ANALYZER_SYSTEM,
        conversation_history=conversation_history_for_ai
    )
    logging.info(f"AI Response is :{response}")
    
    analysis = extract_json_from_text(response) or {}
    
    # Always store analysis (even if partial)
    idea_description = analysis.get("idea_description", "A short professional talking-head video")
    has_script = analysis.get("has_script", False) # Default to False if missing
    
    ti.xcom_push(key="prompt_analysis", value={
        "has_clear_idea": True,  # We force this now
        "idea_description": idea_description,
        "script_quality": "none",
        "suggested_title": analysis.get("suggested_title", "Your Video"),
        "tone": analysis.get("tone", "professional"),
        "aspect_ratio": analysis.get("aspect_ratio", "16:9"),
        "resolution": analysis.get("resolution", "720p")
    })
    
    # Only skip to video if we explicitly have a script
    if has_script:
        logging.info("User provided a script. Routing to Video Generation.")
        ti.xcom_push(key="final_script", value=prompt)
        return "split_script"
    else:
        logging.info(f"No script detected (Idea: {idea_description}). Routing to Script Generation.")
        return "generate_script"

def send_missing_elements_email(**kwargs):
    """Send email about missing elements."""
    ti = kwargs['ti']
    
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input_task")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input_task")
    missing_elements = ti.xcom_pull(key="missing_elements", task_ids="validate_input")
    agent_headers = ti.xcom_pull(key="agent_headers", task_ids="agent_input_task") or {}
    
    headers = email_data.get("headers", {})
    sender_email = headers.get("From", "")

    if not sender_email:
        sender_email = agent_headers.get("X-LTAI-User", "")

    sender_name = "there"
    name_match = re.search(r'^([^<]+)', sender_email)
    if name_match:
        sender_name = name_match.group(1).strip()
        sender_email = sender_email.split("<")[1].replace(">", "").strip()
    
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
    
    # 3. Send Email (ONLY if we have a valid email address)
    if sender_email and "@" in sender_email:
        service = authenticate_gmail()
        if service:
            thread_id = ti.xcom_pull(key="thread_id", task_ids="agent_input_task")
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

def generate_script(**kwargs):
    """Generate video script based on user's idea."""
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input_task")
    chat_history = ti.xcom_pull(key="chat_history", task_ids="agent_input_task")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input_task")
    prompt_analysis = ti.xcom_pull(key="prompt_analysis", task_ids="validate_prompt_clarity")
    AVG_WPM = 140
    
    prompt = email_data.get("content", "").strip()
    idea_description = prompt_analysis.get("idea_description", "")
    
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
    
    constraint_note = ""
    if try_number > 1:
        history = ti.xcom_pull(key='rejected_script_history') or []
        reason = history[-1].get('reason') if history else "Unknown"
        logging.info(f"Retry Reason: {reason}")
        constraint_note = "CRITICAL: Previous script was TOO LONG. Write a significantly shorter version."

    # 4. Prompting
    TARGET_DURATION = 45
    system_instruction = f"""
    You are 'Video Companion'.
    GOAL: Write a video script based on user idea.
    INSTRUCTIONS:
    1. Target: {TARGET_DURATION}s.
    2. Pace: {AVG_WPM} wpm.
    3. Structure: Hook -> Body -> CTA.
    4. {constraint_note}
    Output JSON: {{
        "video_meta": {{ "title": "str", "video_type": "str", "target_duration": {TARGET_DURATION} }},
        "script_content": "text...",
        "visual_direction": "text..."
    }}
    """
    user_prompt = f"IDEA: {prompt}\nSUMMARY: {idea_description}\nGenerate script."

    # 5. Generate & Parse
    response_text = get_gemini_response(user_prompt, system_instruction, conversation_history_for_ai)
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
                thread_id = ti.xcom_pull(key="thread_id", task_ids="agent_input_task")
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
    if not is_agent_trigger(conf):
        return 
    else:
        return 'split_script'


import math

def build_scene_config(segments_data, aspect_ratio="16:9", images=[], max_video_duration=20):
    """
    Transforms AI output into downstream config.
    NOW: Uses the 'duration' provided by the AI, with safety clamping.
    MAX_SCENES is calculated based on max_video_duration.
    """
    # Calculate MAX_SCENES based on video duration
    # Using 8 seconds as the average scene duration for calculation
    AVERAGE_SCENE_DURATION = 8.0
    MAX_SCENES = math.ceil(max_video_duration / AVERAGE_SCENE_DURATION)
    
    # Ensure at least 1 scene
    MAX_SCENES = max(1, MAX_SCENES)
    
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
        logging.info(f"Images : {image_list}")
        logging.info(f"Selected images {selected_image}")
        config_item = {
            "image_path": selected_image.get("path"),
            "prompt": text,
            "duration": final_duration,
            "aspect_ratio": aspect_ratio
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
    
    images = ti.xcom_pull(key='images', task_ids='agent_input_task') 
    # script_content = raw_data.get('script_content')
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input_task")
    chat_history = ti.xcom_pull(key="chat_history", task_ids="agent_input_task")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input_task")
    prompt_analysis = ti.xcom_pull(key="prompt_analysis", task_ids="validate_prompt_clarity")
    aspect_ratio = prompt_analysis.get("aspect_ratio")
    # This one needs fixing - it should handle both script and generated_script
    generate_script = ti.xcom_pull(key="generated_output", task_ids="generate_script", default=None)

    # This line is problematic - it tries to check if generate_script (which could be a dict) equals string content
    text = email_data.get("content", "").strip()
    logging.info(f"{generate_script}, {text}")
    
    user_type = ti.xcom_pull(key="user_type", task_ids="agent_input_task")
    if user_type == "internal":
        MAX_DURATION = Variable.get("CF.video.internal.max_duration", default_var=90)
    elif user_type == "external":
        MAX_DURATION = Variable.get("CF.video.external.max_duration", default_var=90)
    else:
        # Optional: Handle other cases with a default
        MAX_DURATION = 90

    logging.info(f"📥 Processing Script... (Aspect Ratio: {aspect_ratio})")
    # Get the script - either generated or user-provided
   

    # 1. First, check if a script was generated in *this current run* (Script Generation Task)
    final_script = ti.xcom_pull(key="generated_script", task_ids="generate_script")
    
    # 2. If not, look into Chat History (for the MVP "Reply to proceed" flow)
    if not final_script:
        chat_history = ti.xcom_pull(key="chat_history", task_ids="agent_input_task") or []
        
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

def process_single_segment(segment, segment_index, **context):
    """
    Process ONE segment at a time.
    CRITICAL: segment_index preserves the ORDER of video generation.
    """
    ti = context['ti']
    logging.info(f"Processing segment {segment_index}: {segment}")
    
    video_model = Variable.get('CF.video.model', default_var='mock')
    
    if video_model == 'mock':
        mock_list_raw = Variable.get('CF.mock_list', default_var='[]')
        mock_list = json.loads(mock_list_raw) if mock_list_raw else []
        path_index = segment_index % len(mock_list)
        video_path = mock_list[path_index]
        logging.info(f"Mock video {segment_index}: {video_path}")
        
        # Return dict with index to preserve order
        return {
            'index': segment_index,
            'video_path': video_path
        }
    else:
        logging.info(f"Using Google Veo 3.0 for segment {segment_index}")
        try:
            veo_tool = GoogleVeoVideoTool(api_key=GEMINI_API_KEY)
            
            result = veo_tool._run(
                image_path=segment.get('image_path'),
                prompt=segment.get('prompt'),
                aspect_ratio=segment.get('aspect_ratio', '16:9'),
                duration_seconds=segment.get('duration', 6),
                output_dir=Variable.get('CF.OUTPUT_DIR', default_var='/appz/home/airflow/dags')
            )
            
            if result.get('success'):
                video_path = result['video_paths'][0]
                logging.info(f"✅ Generated segment {segment_index}: {video_path}")
                
                return {
                    'index': segment_index,
                    'video_path': video_path
                }
            else:
                raise ValueError(f"Generation failed: {result.get('error')}")
        except Exception as e:
            logging.error(f"Exception in segment {segment_index}: {str(e)}")
            raise

def prepare_segments_for_expand(**context):
    """Convert segments list into format needed for expand with index tracking."""
    ti = context['ti']
    segments = ti.xcom_pull(task_ids='split_script', key='segments')
    
    if not segments:
        logging.warning("No segments found!")
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
        logging.error("❌ No video segments were generated!")
        raise ValueError("No videos to merge")
    
    logging.info(f"📦 Collected {len(segment_results)} video results")
    
    # Sort by index to maintain correct order
    sorted_results = sorted(segment_results, key=lambda x: x['index'])
    
    # Extract video paths in order
    video_paths = [result['video_path'] for result in sorted_results]
    
    logging.info(f"📹 Video paths in order: {video_paths}")
    
    # Validate all videos exist
    valid_paths = [p for p in video_paths if p and Path(p).exists()]
    
    if len(valid_paths) < len(video_paths):
        logging.warning(f"⚠️ Some videos are missing. Expected {len(video_paths)}, found {len(valid_paths)}")
    
    if len(valid_paths) < 2:
        logging.error("❌ Need at least 2 videos to merge")
        raise ValueError("Insufficient videos for merge")
    
    # Generate request ID for merge
    req_id = context['dag_run'].run_id or f"merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Push to XCom for merge task
    merge_params = {
        'video_paths': valid_paths,
        'req_id': req_id
    }
    
    ti.xcom_push(key='merge_params', value=merge_params)
    
    logging.info(f"✅ Ready to merge {len(valid_paths)} videos with req_id: {req_id}")
    
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
    
    logging.info(f"🎬 Starting merge with params: {merge_params}")
    
    # Import the merge logic from the other DAG
    # You might need to adjust the import path based on your structure
    from ffmpg_merger import merge_videos_logic
    
    # Create a modified context with params
    modified_context = context.copy()
    modified_context['params'] = merge_params
    
    # Call the merge function
    result = merge_videos_logic(**modified_context)
    # result = "/appz/home/airflow/dags/video_v1.mp4"
    ti.xcom_push(key="generated_video_path", value=result)
    logging.info(f"✅ Merge complete! Output: {result}")
    
    return result

def send_video_email(**kwargs):
    """Send generated video to user."""
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input_task")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input_task")
    thread_id = ti.xcom_pull(key="thread_id", task_ids="agent_input_task")
    video_path = ti.xcom_pull(key="generated_video_path", task_ids="merge_all_videos")
    
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
    
    email_data = ti.xcom_pull(key="email_data", task_ids="agent_input_task")
    message_id = ti.xcom_pull(key="message_id", task_ids="agent_input_task")
    
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
    description="video_companion_processor:v0.3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["video", "companion", "processor", "conversational"]
) as dag:

    agent_input_task = BranchPythonOperator(
        task_id="agent_input_task",
        python_callable=agent_input_task,
        provide_context=True
    )
    freemium_guard_task = BranchPythonOperator(
        task_id="freemium_guard_task",
        python_callable=freemium_guard_task,
        provide_context=True
    )
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

    # send_unclear_idea_task = PythonOperator(
    #     task_id="send_unclear_idea_email",
    #     python_callable=send_unclear_idea_email,
    #     provide_context=True
    # )

    generate_script_task = BranchPythonOperator(  # ✅ Correct for branching
        task_id="generate_script",
        python_callable=generate_script,
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
    
    split_script = PythonOperator(
        task_id='split_script',
        python_callable=split_script_task,
        dag=dag,
        trigger_rule="none_failed_min_one_success"
    )

    prepare_segments = PythonOperator(
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
    ).expand(op_kwargs=prepare_segments.output)

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
    
    # Task dependencies
    agent_input_task >> freemium_guard_task >> validate_input_task >> [validate_prompt_clarity_task, send_missing_elements_task]

    # From validate_prompt_clarity, branch to different paths
    validate_prompt_clarity_task >> [
        generate_script_task,
        split_script,  # Direct path when script provided
        send_error_email_task
    ]

    # Script generation path (leads to split_script OR end)
    generate_script_task >> split_script
    generate_script_task >> end_task

    # Video processing pipeline (starts from split_script)
    split_script >> prepare_segments >> process_segments >> collect_task >> merge_task >> send_video_email 

    # Error/completion paths
    send_missing_elements_task >> end_task
    send_error_email_task >> end_task