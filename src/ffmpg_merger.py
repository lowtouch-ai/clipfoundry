"""
DAG: video_merger_pipeline
Author: lowtouch.ai Engineering
Description: Merges video clips with High Fidelity intermediates, Smart Audio Mapping, 
             Robust Transitions (Anti-Drift), and Branding Watermarks.
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

# DURATION OF THE FADE (Increase to 1.0s if you want it more noticeable)
TRANSITION_DURATION = 0.5 

# WATERMARK PATHS (Ensure these exist in your shared storage)
WATERMARK_PATH_16_9 = "/appz/shared/branding/watermark_16_9.mp4"
WATERMARK_PATH_9_16 = "/appz/shared/branding/watermark_9_16.mp4"

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
    Uses ffprobe to extract width, height, duration AND check for audio stream.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "stream=width,height,duration,codec_type",
        "-of", "json",
        str(file_path)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        video_stream = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), None)
        audio_stream = next((s for s in data.get('streams', []) if s['codec_type'] == 'audio'), None)
        
        if not video_stream:
            raise ValueError(f"No video stream found in {file_path}")

        return {
            'width': int(video_stream.get('width', 0)),
            'height': int(video_stream.get('height', 0)),
            'duration': float(video_stream.get('duration', 0)),
            'has_audio': audio_stream is not None
        }
    except Exception as e:
        logger.error(f"Failed to probe {file_path}: {e}")
        raise

def prepare_segment(source_path: str, output_path: str, target_width: int, target_height: int) -> float:
    """
    Standardizes a video segment using High Quality settings & Smart Audio Mapping.
    """
    # 1. Probe to check for audio
    meta = get_video_metadata(source_path)
    has_audio = meta['has_audio']

    vf_filter = (
        f"scale={target_width}:{target_height}:force_original_aspect_ratio=decrease,"
        f"pad={target_width}:{target_height}:(ow-iw)/2:(oh-ih)/2"
    )
    
    # Base command
    cmd = ["ffmpeg", "-y", "-i", source_path]
    
    # AUDIO LOGIC ------------------------------------------
    if has_audio:
        # Case A: Source has audio. Use it explicitly.
        cmd.extend(["-map", "0:v", "-map", "0:a"])
    else:
        # Case B: Source is silent. Generate silence to prevent sync issues.
        cmd.extend([
            "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
            "-map", "0:v", "-map", "1:a", # Map video from file, audio from generator
            "-shortest" # Stop generator when video ends
        ])
    # ------------------------------------------------------

    cmd.extend([
        "-vf", vf_filter,
        # HIGH QUALITY INTERMEDIATE SETTINGS
        "-c:v", "libx264", 
        "-preset", "fast",     
        "-crf", "18",          # Visually lossless
        "-pix_fmt", "yuv420p", # Essential for player compatibility
        "-r", "30",
        # HIGH QUALITY AUDIO
        "-c:a", "aac", "-b:a", "320k", "-ar", "48000", "-ac", "2",
        output_path
    ])
    
    logger.info(f"Standardizing segment (HQ, Audio={has_audio}): {source_path} -> {output_path}")
    subprocess.run(cmd, check=True, capture_output=True)
    
    new_meta = get_video_metadata(output_path)
    return new_meta['duration']

def merge_pair(video_a: str, video_b: str, output_path: str, duration_a: float) -> float:
    """
    Merges two video files (A + B) with High Fidelity and drift-proof timestamps.
    """
    offset = duration_a - TRANSITION_DURATION
    if offset < 0: offset = 0
    
    # --- THE FIX: Force timestamps to 0 to prevent fade failure ---
    # We use setpts=PTS-STARTPTS to reset the clock for both inputs before blending.
    filter_complex = (
        f"[0:v]setpts=PTS-STARTPTS[v0];"
        f"[1:v]setpts=PTS-STARTPTS[v1];"
        f"[v0][v1]xfade=transition=fade:duration={TRANSITION_DURATION}:offset={offset}[v];"
        
        f"[0:a]asetpts=PTS-STARTPTS[a0];"
        f"[1:a]asetpts=PTS-STARTPTS[a1];"
        f"[a0][a1]acrossfade=d={TRANSITION_DURATION}:c1=tri:c2=tri[a]"
    )

    
    cmd = [
        "ffmpeg", "-y",
        "-i", video_a,
        "-i", video_b,
        "-filter_complex", filter_complex,
        "-map", "[v]", "-map", "[a]",
        # HIGH QUALITY INTERMEDIATE SETTINGS
        "-c:v", "libx264", 
        "-preset", "fast", 
        "-crf", "18",           
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "320k",
        str(output_path)
    ]
    
    logger.info(f"Merging pair (HQ): {Path(video_a).name} + {Path(video_b).name} (Offset: {offset}s)")
    subprocess.run(cmd, check=True, capture_output=True)
    
    return get_video_metadata(output_path)['duration']

def add_fades(input_path: str, output_path: str, duration: float):
    """Adds fade out to the final consolidated video (Distribution Quality)."""
    FADE_DURATION = 1.0
    start_time = duration - FADE_DURATION
    if start_time < 0: start_time = 0

    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-vf", f"fade=t=out:st={start_time}:d={FADE_DURATION}",
        "-af", f"afade=t=out:st={start_time}:d={FADE_DURATION}",
        # FINAL DISTRIBUTION SETTINGS
        "-c:v", "libx264", 
        "-preset", "medium",    # Better compression for final file
        "-crf", "23",           # Standard web quality
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        str(output_path)
    ]
    
    logger.info(f"Applying final fades: {output_path}")
    subprocess.run(cmd, check=True, capture_output=True)


def merge_videos_logic(**context):
    params = context['params']
    
    # 1. Parse Inputs
    raw_paths = params.get('video_paths', [])
    if isinstance(raw_paths, str):
        try:
            video_paths = json.loads(raw_paths)
        except json.JSONDecodeError:
            video_paths = [p.strip() for p in raw_paths.split(',')]
    else:
        video_paths = raw_paths

    req_id = params.get('req_id', 'manual_test')
    valid_paths = [p for p in video_paths if os.path.exists(p)]
    if len(valid_paths) < 1: # Allow 1 video if we are just appending watermark
        logger.warning("No valid videos to merge.")
        return None

    # Setup Workspaces
    if 'work_dir' in params:
        base_dir = Path(params['work_dir'])
    else:
        base_dir = Path(f"/appz/shared/{req_id}")
    temp_dir = base_dir / "temp"
    output_dir = params.get('output_dir', base_dir / "output")
    output_dir = Path(output_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 2. Determine Target Resolution
    meta_first = get_video_metadata(valid_paths[0])
    target_w = meta_first['width']
    target_h = meta_first['height']
    logger.info(f"Target Resolution locked to: {target_w}x{target_h}")

    # 3. Process Segments (Standardization)
    processed_clips = []
    
    for idx, raw_clip in enumerate(valid_paths):
        out_name = temp_dir / f"norm_{idx:03d}.mp4"
        duration = prepare_segment(str(raw_clip), str(out_name), target_w, target_h)
        processed_clips.append({'path': str(out_name), 'duration': duration})

    # 4. Iterative Merge
    current_merged_path = processed_clips[0]['path']
    current_duration = processed_clips[0]['duration']

    if len(processed_clips) > 1:
        for i in range(1, len(processed_clips)):
            next_clip = processed_clips[i]
            step_output = temp_dir / f"step_merge_{i}.mp4"
            
            new_duration = merge_pair(
                current_merged_path, 
                next_clip['path'], 
                str(step_output), 
                current_duration
            )
            
            current_merged_path = str(step_output)
            current_duration = new_duration

    # --- 4.5 ADD WATERMARK (BRANDING) ---
    # Select path based on aspect ratio
    aspect_ratio = target_w / target_h
    if aspect_ratio > 1.0:
        # Landscape (16:9)
        watermark_source = WATERMARK_PATH_16_9
    else:
        # Portrait (9:16)
        watermark_source = WATERMARK_PATH_9_16

    if os.path.exists(watermark_source):
        logger.info(f"Appending branding watermark: {watermark_source}")
        
        # Standardize Watermark (ensure resolution match)
        wm_norm_path = temp_dir / "norm_watermark.mp4"
        wm_duration = prepare_segment(watermark_source, str(wm_norm_path), target_w, target_h)
        
        # Merge Watermark into final video (Transition handles fade in)
        wm_merge_output = temp_dir / "step_merge_branding.mp4"
        new_duration = merge_pair(
            current_merged_path,
            str(wm_norm_path),
            str(wm_merge_output),
            current_duration
        )
        
        current_merged_path = str(wm_merge_output)
        current_duration = new_duration
    else:
        logger.warning(f"Watermark file missing at {watermark_source}. Skipping branding.")

    # 5. Final Polish
    final_output_name = f"merged_{req_id}.mp4"
    final_output_path = output_dir / final_output_name
    
    add_fades(current_merged_path, str(final_output_path), current_duration)    
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