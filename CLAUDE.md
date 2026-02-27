# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ClipFoundry is an autonomous video production agent built on Apache Airflow. It accepts requests via email or chat (OpenWebUI), uses Google Gemini for script generation/analysis, Google Veo for AI video synthesis, and FFmpeg for final video assembly. The system runs as a self-contained appliance deployed at `/appz/home/airflow/dags/clipfoundry` in production.

## Architecture

### Three Production DAGs

1. **`video_companion_monitor_mailbox`** (`src/video_genrator_email_listener.py`)
   - Runs every 1 minute, polls Gmail inbox for new emails
   - Validates senders against `CF.email.whitelist` Airflow Variable
   - Creates/reuses per-thread workspaces at `/appz/shared_images/{thread_id}/`
   - Saves email image attachments, builds full thread history
   - Triggers `video_companion_processor` for each valid email

2. **`video_companion_processor`** (`src/video_companion_processor.py`)
   - Core orchestration DAG — handles both email and agent (OpenWebUI) triggers
   - Uses `is_agent_trigger()` to detect trigger source (agent has `agent_headers`, email has `From` header)
   - Task pipeline: `agent_input → freemium_guard → validate_input → validate_prompt_clarity → [generate_script | split_script] → check_pg13 → prepare_segments → process_segment (parallel, pool=video_processing_pool) → collect_videos → merge_all_videos → send_video`
   - Script generation uses Gemini; video generation uses `GoogleVeoVideoTool` or mock mode
   - Sends results via Gmail reply (email path) or MinIO download link

3. **`video_merge_pipeline_enhanced`** (`src/ffmpg_merger.py`)
   - Standalone DAG for merging pre-existing video clips
   - Uses `ffprobe` for metadata, detects silence, applies fade transitions (0.1s), adds watermarks
   - Watermarks at `/appz/shared/branding/watermark_16_9.mp4` and `watermark_9_16.mp4`

### Key Modules

- **`src/agent/veo.py`** — `GoogleVeoVideoTool` (LangChain `BaseTool`): wraps Google Veo 3.0 API. Constructs a 7-component meta-prompt per scene. Model: `veo-3.0-fast-generate-001`. Supports `negative_prompt` field.
- **`src/models/gemini.py`** — `get_gemini_response()`: thin wrapper over `google-genai` SDK. Supports `response_format` (`"json"`, `"markdown"`, `"text"`), conversation history, system instructions.
- **`src/email_utils/email.py`** — Gmail send/auth utilities, used by processor DAG for replies.

### Storage Layout (Production)

```
/appz/
  shared_images/{thread_id}/    # Per-email-thread workspace
    images/                     # input_1.jpg, input_2.png, ...
    input/
    output/
  shared/branding/              # Watermark video files
  cache/                        # Timestamp tracking files
  logs/
```

### Airflow Variables (Required)

| Variable | Purpose |
|---|---|
| `CF.companion.from.address` | Gmail address used as bot identity |
| `CF.companion.gmail.credentials` | OAuth2 credentials JSON string |
| `CF.companion.gemini.api_key` | Gemini API key |
| `CF.companion.gemini.model` | Default: `gemini-2.5-flash` |
| `CF.email.whitelist` | JSON array of allowed sender emails |
| `CF.webui.base_url` | Default: `https://chat.clipfoundry.ai` |
| `CF.video.model` | `mock` or `veo` — controls video engine |
| `CF.mock_list` | JSON array of mock video paths for testing |
| `CF.script.wpm` | Words per minute for script length calc (default: 170) |
| `CF.video.internal.max_duration` | Max video seconds for internal users (default: 90) |
| `CF.video.external.max_duration` | Max video seconds for external users (default: 90) |
| `CF.video.external.limit` | Usage cap for external users (default: 5) |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | MinIO credentials for asset storage |

## Development Workflow

### Running/Testing DAGs

DAGs are deployed directly to Airflow by copying files to `/appz/home/airflow/dags/clipfoundry/src/`. There is no build step.

Test scripts in `src/test_scrips/` are standalone Airflow DAGs for isolated feature testing — they are NOT unit tests and must be triggered via the Airflow UI or CLI:

```bash
# Trigger a DAG run manually
airflow dags trigger video_companion_processor

# Test with mock video generation (set CF.video.model = "mock" in Airflow Variables)
# Then trigger the DAG with a test conf payload
```

### Mock Mode

Set the Airflow Variable `CF.video.model` to `"mock"` during development. The processor will use pre-recorded videos from `CF.mock_list` instead of calling the Veo API.

### Python Path

`video_companion_processor.py` manually adds its own directory and the dags root to `sys.path` to import `agent.veo`, `models.gemini`, and `agent_workflows.utils.think_logging` (the last is an external shared library from the broader Airflow environment).

## Key Design Patterns

- **Thread = Workspace**: Gmail `threadId` is used directly as the workspace directory name under `/appz/shared_images/`. No mapping file needed.
- **XCom for state**: Tasks pass data between each other via Airflow XCom. The email processor pushes `unread_emails` list; the processor DAG uses `conf` dict from the trigger payload.
- **Dynamic task expansion**: `process_segment` uses `PythonOperator.partial(...).expand()` for parallel per-scene video generation with `pool="video_processing_pool"`.
- **Dual trigger paths**: `is_agent_trigger()` checks for `agent_headers` key in `conf` to distinguish between email-triggered and chat-triggered runs, routing to different response paths.
- **Gemini response format**: Always pass `response_format="json"` when expecting structured data. The LLM prompts include explicit JSON schema requirements; use `extract_json_from_text()` as a fallback parser.
