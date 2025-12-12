"""
DAG: video_merger_pipeline
Author: lowtouch.ai Engineering
Description: Merges video clips with natural xfade transitions and audio crossfades.
             Handles dynamic resolutions (Landscape/Reels) and prevents audio gaps.
"""

import json
import shutil
import subprocess
import os
from pathlib import Path
from typing import List, Dict, Any
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.dates import days_ago

# ================= configuration =================
logger = logging.getLogger("airflow.task")

# Configurable transition overlap duration (seconds)
TRANSITION_DURATION = 0.5 

default_args = {
    'owner': 'lowtouch-ai',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# ================= helper functions =================

def get_video_metadata(file_path: str) -> Dict[str, Any]:
    """
    Uses ffprobe to extract width, height, and exact duration.
    """
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

def prepare_segment(
    source_path: str, 
    output_path: str, 
    target_width: int, 
    target_height: int
) -> float:
    """
    Standardizes a video segment:
    1. Scales/Pads to match target resolution (keeping aspect ratio).
    2. Normalizes audio to AAC 48k to prevent breaks.
    3. Sets a standard frame rate.
    
    Returns: The exact duration of the processed clip.
    """
    # Filter: Scale to fit target box, then pad with black to fill target box
    vf_filter = (
        f"scale={target_width}:{target_height}:force_original_aspect_ratio=decrease,"
        f"pad={target_width}:{target_height}:(ow-iw)/2:(oh-ih)/2"
    )
    
    cmd = [
        "ffmpeg", "-y",
        "-i", source_path,
        "-vf", vf_filter,
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
        "-r", "30",              # Standardize Framerate
        "-c:a", "aac", "-b:a", "192k", "-ar", "48000", # Standardize Audio
        output_path
    ]
    
    logger.info(f"Standardizing segment: {source_path} -> {output_path}")
    subprocess.run(cmd, check=True, capture_output=True)
    
    # Return new duration (encoding might shift it slightly)
    meta = get_video_metadata(output_path)
    return meta['duration']

def merge_videos_logic(**context):
    """
    Main Logic:
    1. Parse inputs.
    2. Determine Target Resolution (from first clip).
    3. Pre-process all clips (Standardize).
    4. Construct complex xfade/acrossfade filter command.
    5. Execute Merge.
    """
    params = context['params']
    
    # 1. Parse Inputs
    # Handle the case where input might be a string representation of a list or a real list
    raw_paths = params.get('video_paths', [])
    if isinstance(raw_paths, str):
        try:
            video_paths = json.loads(raw_paths)
        except json.JSONDecodeError:
            # Fallback for comma separated string
            video_paths = [p.strip() for p in raw_paths.split(',')]
    else:
        video_paths = raw_paths

    req_id = params.get('req_id', 'manual_test')
    
    if not video_paths:
        raise ValueError("No video paths provided in params.")

    # Validate paths exist
    valid_paths = []
    for p in video_paths:
        if os.path.exists(p):
            valid_paths.append(p)
        else:
            logger.warning(f"File not found, skipping: {p}")
    
    if len(valid_paths) < 2:
        logger.info("Less than 2 valid videos. No merge needed.")
        return valid_paths[0] if valid_paths else None

    # Setup Workspaces
    # If req_id is provided, output to: /appz/shared/{req_id}/output/merged_{req_id}.mp4
    # Working dir: /appz/shared/{req_id}/temp
    base_dir = Path(f"/appz/shared/{req_id}") if req_id != 'manual_test' else Path(f"/appz/shared/mock")
    temp_dir = base_dir / "temp"
    output_dir = base_dir / "output" if req_id != 'manual_test' else base_dir / "input"
    
    temp_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 2. Determine Target Resolution from Clip 1
    meta_first = get_video_metadata(valid_paths[0])
    target_w = meta_first['width']
    target_h = meta_first['height']
    logger.info(f"Target Resolution locked to: {target_w}x{target_h}")

    # 3. Process Segments & Collect Exact Durations
    processed_clips = []
    clip_durations = []
    
    for idx, raw_clip in enumerate(valid_paths):
        out_name = temp_dir / f"norm_{idx:03d}.mp4"
        duration = prepare_segment(str(raw_clip), str(out_name), target_w, target_h)
        processed_clips.append(str(out_name))
        clip_durations.append(duration)
        logger.info(f"Clip {idx} duration: {duration}s")

    # 4. Build FFmpeg Filter Complex
    # We iterate N-1 times for transitions.
    
    inputs = []
    for clip in processed_clips:
        inputs.extend(["-i", clip])

    filter_parts = []
    
    # --- Video Transitions (xfade) ---
    # We must track the "start time" of the NEXT clip relative to the timeline.
    # offset = current_end_time - TRANSITION_DURATION
    
    cumulative_duration = 0.0
    current_v_label = "0:v"
    
    # Initialize with first clip duration
    cumulative_duration = clip_durations[0]

    for i in range(len(processed_clips) - 1):
        next_v_idx = i + 1
        next_v_label = f"{next_v_idx}:v"
        
        # The offset is where the transition BEGINS.
        offset = cumulative_duration - TRANSITION_DURATION
        
        # Safety check: offset cannot be negative
        if offset < 0: offset = 0
        
        # Transition type (can be randomized or fixed)
        trans_type = "fade" 
        
        target_v_label = f"v{i}" if i < len(processed_clips) - 2 else "vout"
        
        xfade_cmd = (
            f"[{current_v_label}][{next_v_label}]"
            f"xfade=transition={trans_type}:duration={TRANSITION_DURATION}:offset={offset}"
            f"[{target_v_label}]"
        )
        filter_parts.append(xfade_cmd)
        
        current_v_label = target_v_label
        
        # Update duration for the next loop.
        # The new total length is: (current_timeline + next_clip_duration - overlap)
        # Note: xfade logic uses absolute timeline offset.
        # So we just add the non-overlapped part of the NEXT clip.
        cumulative_duration += (clip_durations[next_v_idx] - TRANSITION_DURATION)

    # --- Audio Transitions (acrossfade) ---
    # acrossfade does NOT use offsets. It just says "fade the end of A into start of B".
    # Since we are feeding them sequentially, we just chain them.
    
    current_a_label = "0:a"
    for i in range(len(processed_clips) - 1):
        next_a_idx = i + 1
        next_a_label = f"{next_a_idx}:a"
        target_a_label = f"a{i}" if i < len(processed_clips) - 2 else "aout"
        
        # Audio doesn't need 'offset' calc, it just overlaps ends
        afade_cmd = (
            f"[{current_a_label}][{next_a_label}]"
            f"acrossfade=d={TRANSITION_DURATION}:c1=tri:c2=tri"
            f"[{target_a_label}]"
        )
        filter_parts.append(afade_cmd)
        current_a_label = target_a_label

    full_filter = ";".join(filter_parts)

    # 5. Execute Merge
    final_output_name = f"merged_{req_id}.mp4" if req_id != 'manual_test' else "merged_mock.mp4"
    final_output_path = output_dir / final_output_name
    
    merge_cmd = [
        "ffmpeg", "-y",
        *inputs,
        "-filter_complex", full_filter,
        "-map", "[vout]", "-map", "[aout]",
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        str(final_output_path)
    ]
    
    logger.info(f"Running Final Merge: {final_output_path}")
    result = subprocess.run(merge_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"FFmpeg Merge Failed: {result.stderr}")
        raise RuntimeError("FFmpeg merge failed")
        
    logger.info(f"Successfully created: {final_output_path}")
    
    # Cleanup Temp
    try:
        shutil.rmtree(temp_dir)
    except:
        pass

    return str(final_output_path)


# ================= DAG Definition =================

with DAG(
    dag_id='video_merge_pipeline',
    default_args=default_args,
    description='Merges videos with xfade and acrossfade handling varying resolutions',
    schedule_interval=None,  # Manual Trigger
    start_date=days_ago(1),
    tags=['lowtouch', 'video', 'ffmpeg'],
    params={
        "video_paths": Param(
            default=["/appz/shared/mock/part1.mp4", "/appz/shared/mock/part2.mp4"],
            type="array",
            description="List of absolute file paths to merge in order."
        ),
        "req_id": Param(
            default="manual_test",
            type="string",
            description="Request ID for folder structuring."
        )
    }
) as dag:

    merge_task = PythonOperator(
        task_id='merge_video_files',
        python_callable=merge_videos_logic,
        provide_context=True
    )

    merge_task