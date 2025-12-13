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

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

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

# Use the appropriate Gemini model (e.g. gemini-1.5-flash or gemini-1.5-pro)
GEMINI_MODEL = Variable.get("video.companion.gemini.model", default_var="gemini-2.5-flash")
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
    """
    Validate input from both Email and Agent sources.
    """
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
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
    ti = kwargs['ti']
    ti.xcom_push(key="agent_headers", value=agent_headers)
    
    # Update email_data in XCom to ensure downstream tasks find the content
    # regardless of where it came from
    updated_email_data = email_data.copy()
    updated_email_data["content"] = prompt
    ti.xcom_push(key="email_data", value=updated_email_data)
    ti.xcom_push(key="images", value=images)
    ti.xcom_push(key="chat_history", value=chat_history)
    ti.xcom_push(key="thread_id", value=thread_id)
    ti.xcom_push(key="message_id", value=message_id)
    
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
        logging.warning(f"Missing: {missing}")
        ti.xcom_push(key="validation_status", value="missing_elements")
        ti.xcom_push(key="missing_elements", value=missing)
        
        # Check Source
        is_agent = bool(agent_headers) or not bool(email_data.get("headers", {}).get("From"))
        
        if is_agent:
            logging.info("Agent Trigger + Missing Info -> Stopping DAG (Agent will read XCom)")
            return "end" # Bypass email task
        else:
            return "send_missing_elements_email"
    
    logging.info("Input valid")
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
        "aspect_ratio": analysis.get("aspect_ratio", "720p"),
        "resolution": analysis.get("resolution", "16:9")
    })
    
    # Only skip to video if we explicitly have a script
    if has_script:
        logging.info("User provided a script. Routing to Video Generation.")
        ti.xcom_push(key="final_script", value=prompt)
        return "generate_video"
    else:
        logging.info(f"No script detected (Idea: {idea_description}). Routing to Script Generation.")
        return "generate_script"

def send_missing_elements_email(**kwargs):
    """Send email about missing elements."""
    ti = kwargs['ti']
    
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="validate_input")
    missing_elements = ti.xcom_pull(key="missing_elements", task_ids="validate_input")
    agent_headers = ti.xcom_pull(key="agent_headers", task_ids="validate_input") or {}
    
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
            send_email(service, sender_email, subject, html_content, thread_headers=headers)
            
            # 4. Mark Read (ONLY if we have a valid Message ID)
            if message_id:
                mark_message_as_read(service, message_id)
            
            logging.info(f"Sent missing elements email to {sender_email}")
    else:
        logging.info("No valid recipient email found. Skipping email send.")

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
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    chat_history = ti.xcom_pull(key="chat_history", task_ids="validate_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="validate_input")
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
                send_email(service, sender_email_addr, subject, body, thread_headers=headers)
                mark_message_as_read(service, message_id)
                logging.info(f"Script approval email sent to {sender_email_addr}")
    else:
        logging.info("Agent trigger detected: Skipping script approval email.")

    # 9. Return Unified Payload
    ti.xcom_push(key="generated_script", value=script)
    return {
        "status": "success",
        "markdown_output": agent_markdown,
        "email_html": email_html, 
        "raw_data": data
    }

def generate_video(**kwargs):
    """Generate video by triggering the video generation DAG."""
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    email_data = ti.xcom_pull(key="email_data", task_ids="validate_input")
    images = ti.xcom_pull(key="images", task_ids="validate_input")
    thread_id = ti.xcom_pull(key="thread_id", task_ids="validate_input")
    message_id = ti.xcom_pull(key="message_id", task_ids="validate_input")
    prompt_analysis = ti.xcom_pull(key="prompt_analysis", task_ids="validate_prompt_clarity")
    
    # Get the script - either generated or user-provided
    generated_script_result = ti.xcom_pull(key="generated_script", task_ids="generate_script")
    user_script = email_data.get("content", "").strip()
    
    # Use generated script if available, otherwise use user's original prompt as script
    final_script = generated_script_result if generated_script_result else user_script
    
    if not final_script:
        logging.error("No script available for video generation")
        ti.xcom_push(key="video_generation_error", value="No script available")
        ti.xcom_push(key="video_generation_success", value=False)
        return
    
    logging.info(f"Triggering video generation DAG for thread {thread_id}")
    
    # Get image paths
    image_paths = [img["path"] for img in images if os.path.exists(img.get("path", ""))]
    
    if not image_paths:
        logging.error("No valid image paths found")
        ti.xcom_push(key="video_generation_error", value="No images available")
        ti.xcom_push(key="video_generation_success", value=False)
        return
    
    logging.info(f"Using {len(image_paths)} images: {image_paths}")
    
    # Prepare configuration for video generation DAG
    video_dag_config = {
        "script_content": final_script,
        "images": image_paths,
        "aspect_ratio": prompt_analysis.get("aspect_ratio", "16:9"),
        "resolution": prompt_analysis.get("resolution", "720p"),
        "thread_id": thread_id,
        "message_id": message_id,
        # Email metadata for video DAG to send completion email
        # "requester_email": email_data.get("headers", {}).get("From", ""),
        # "original_subject": email_data.get("headers", {}).get("Subject", ""),
        # "email_headers": email_data.get("headers", {}),
        "email_data":email_data,
        "triggered_by": "script_builder",
        "trigger_source": "agent" if is_agent_trigger(conf) else "email"
    }
    
    try:
        from airflow.api.common.trigger_dag import trigger_dag
        
        # Trigger the video generation and merge pipeline DAG
        run_id = f"triggered_from_script_builder_{thread_id}_{int(datetime.now().timestamp())}"
        
        trigger_dag(
            dag_id="video_generation_and_merge_pipeline",
            run_id=run_id,
            conf=video_dag_config,
            execution_date=None,
            replace_microseconds=False
        )
        
        logging.info(f"✅ Successfully triggered video generation DAG with run_id: {run_id}")
        
        # Store the run_id for tracking
        ti.xcom_push(key="triggered_dag_run_id", value=run_id)
        ti.xcom_push(key="video_generation_triggered", value=True)
        ti.xcom_push(key="video_generation_success", value=True)
        
        # If this is an agent trigger, return info about the triggered DAG
        if is_agent_trigger(conf):
            return {
                "status": "video_generation_triggered",
                "dag_run_id": run_id,
                "message": f"Video generation pipeline started. Track progress with run_id: {run_id}"
            }
        
    except Exception as e:
        logging.error(f"Failed to trigger video generation DAG: {e}", exc_info=True)
        ti.xcom_push(key="video_generation_error", value=str(e))
        ti.xcom_push(key="video_generation_success", value=False)
        raise




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
    "script_builder",
    description="video_companion_processor:v0.3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md=readme_content,
    tags=["video", "companion", "processor", "conversational"]
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
    
    generate_video_task >> end_task
    
    send_missing_elements_task >> end_task
    send_unclear_idea_task >> end_task
    send_error_email_task >> end_task