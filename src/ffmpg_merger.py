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
from typing import List, Dict, Any, Tuple
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.dates import days_ago
import shutil
from datetime import datetime
from airflow.models import Variable
# ================= configuration =================
logger = logging.getLogger("airflow.task")

# DURATION OF THE FADE (Increase to 1.0s if you want it more noticeable)
TRANSITION_DURATION = 0.1

# Silence detection thresholds
SILENCE_THRESHOLD = Variable.get("CF.merger.silence.threshold", default_var="-30dB")  # Audio level threshold for silence detection
SILENCE_MIN_DURATION = Variable.get("CF.merger.silence.min_duration", default_var="0.3")   # Minimum duration (seconds) to consider as silence

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


def detect_silence_boundaries(file_path: str) -> Tuple[float, float]:
    """
    Detects silence at the start and end of an audio/video file.
    Returns (start_trim, end_trim) in seconds.
    
    Uses silencedetect filter to find silent regions, then calculates
    how much to trim from start and end.
    """
    cmd = [
        "ffmpeg", "-i", file_path,
        "-af", f"silencedetect=n={SILENCE_THRESHOLD}:d={SILENCE_MIN_DURATION}",
        "-f", "null", "-"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        stderr = result.stderr
        
        # Parse silence detection output
        silence_start_times = []
        silence_end_times = []
        
        for line in stderr.split('\n'):
            if 'silencedetect' in line:
                if 'silence_start' in line:
                    try:
                        time_str = line.split('silence_start:')[1].strip().split()[0]
                        silence_start_times.append(float(time_str))
                    except (IndexError, ValueError):
                        continue
                elif 'silence_end' in line:
                    try:
                        time_str = line.split('silence_end:')[1].strip().split()[0]
                        silence_end_times.append(float(time_str))
                    except (IndexError, ValueError):
                        continue
        
        # Get total duration
        meta = get_video_metadata(file_path)
        total_duration = meta['duration']
        
        # Calculate trim points
        start_trim = 0.0
        end_trim = 0.0
        
        # If silence starts at beginning (within first 0.1s), trim it
        if silence_start_times and silence_start_times[0] < 0.1:
            if silence_end_times:
                start_trim = silence_end_times[0]
        
        # If silence extends to the end, trim it
        if silence_end_times and silence_end_times[-1] > (total_duration - 0.5):
            # Find the last non-silent point
            if len(silence_start_times) >= len(silence_end_times):
                end_trim = total_duration - silence_start_times[-1]
            else:
                # If we have more ends than starts, use the second-to-last end
                if len(silence_end_times) > 1:
                    end_trim = total_duration - silence_start_times[-1]
        
        logger.info(f"Detected silence boundaries for {Path(file_path).name}: "
                   f"start_trim={start_trim:.3f}s, end_trim={end_trim:.3f}s")
        
        return start_trim, end_trim
        
    except Exception as e:
        logger.warning(f"Failed to detect silence in {file_path}: {e}. Using no trim.")
        return 0.0, 0.0


def trim_silence(input_path: str, output_path: str, start_trim: float, end_trim: float) -> float:
    """
    Trims silence from the beginning and end of a video file.
    Returns the new duration after trimming.
    """
    meta = get_video_metadata(input_path)
    original_duration = meta['duration']
    
    # Calculate new duration
    new_duration = original_duration - start_trim - end_trim
    
    if new_duration <= 0.1:
        logger.warning(f"Trimming would result in duration <= 0.1s. Skipping trim for {input_path}")
        shutil.copy2(input_path, output_path)
        return original_duration
    
    # Use trim filters for precise cutting
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start_trim),  # Start time
        "-i", input_path,
        "-t", str(new_duration),  # Duration
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "320k",
        "-ar", "48000",
        "-ac", "2",
        output_path
    ]
    
    logger.info(f"Trimming {Path(input_path).name}: {original_duration:.2f}s -> {new_duration:.2f}s "
               f"(removed {start_trim:.2f}s from start, {end_trim:.2f}s from end)")
    
    subprocess.run(cmd, check=True, capture_output=True)
    return new_duration


def prepare_segment(source_path: str, output_path: str, target_width: int, target_height: int, 
                    remove_silence: bool = True) -> float:
    """
    Standardizes a video segment with optional silence removal.
    """
    meta = get_video_metadata(source_path)
    has_audio = meta['has_audio']
    
    # Create temporary path for silence-trimmed version
    temp_trimmed = None
    if remove_silence and has_audio:
        temp_trimmed = str(Path(output_path).parent / f"trim_{Path(output_path).name}")
        start_trim, end_trim = detect_silence_boundaries(source_path)
        
        if start_trim > 0.01 or end_trim > 0.01:  # Only trim if significant
            trim_silence(source_path, temp_trimmed, start_trim, end_trim)
            source_path = temp_trimmed  # Use trimmed version for standardization

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
    
    logger.info(f"Standardizing segment (HQ, Audio={has_audio}, SilenceRemoval={remove_silence}): "
               f"{Path(source_path).name} -> {Path(output_path).name}")
    subprocess.run(cmd, check=True, capture_output=True)
    
    # Cleanup temp trimmed file
    if temp_trimmed and os.path.exists(temp_trimmed):
        os.remove(temp_trimmed)
    
    new_meta = get_video_metadata(output_path)
    return new_meta['duration']


def merge_pair_concat_audio(video_a: str, video_b: str, output_path: str, duration_a: float) -> float:
    """
    Merges two video files with video crossfade but CONCATENATED audio (no overlap).
    This prevents audio bleeding between segments.
    """
    offset = duration_a - TRANSITION_DURATION
    if offset < 0: offset = 0
    
    # Video: smooth crossfade
    # Audio: direct concatenation at the transition point (no overlap)
    filter_complex = (
        f"[0:v]setpts=PTS-STARTPTS[v0];"
        f"[1:v]setpts=PTS-STARTPTS[v1];"
        f"[v0][v1]xfade=transition=fade:duration={TRANSITION_DURATION}:offset={offset}[v];"
        
        # Audio concatenation instead of crossfade
        f"[0:a]asetpts=PTS-STARTPTS[a0];"
        f"[1:a]asetpts=PTS-STARTPTS[a1];"
        f"[a0][a1]concat=n=2:v=0:a=1[a]"
    )
    
    cmd = [
        "ffmpeg", "-y",
        "-i", video_a,
        "-i", video_b,
        "-filter_complex", filter_complex,
        "-map", "[v]", "-map", "[a]",
        "-c:v", "libx264", 
        "-preset", "fast", 
        "-crf", "18",           
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "320k",
        str(output_path)
    ]
    
    logger.info(f"Merging pair with concat audio (HQ): {Path(video_a).name} + {Path(video_b).name}")
    subprocess.run(cmd, check=True, capture_output=True)
    
    return get_video_metadata(output_path)['duration']


def finalize_video(input_path: str, output_path: str):
    """
    Finalizes the video for distribution.
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-c:v", "libx264", 
        "-preset", "medium",
        "-crf", "23",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        str(output_path)
    ]
    
    logger.info(f"Finalizing video: {output_path}")
    subprocess.run(cmd, check=True, capture_output=True)


def merge_videos_logic(**context):
    params = context['params']
    
    # Parse Inputs
    raw_paths = params.get('video_paths', [])
    if isinstance(raw_paths, str):
        try:
            video_paths = json.loads(raw_paths)
        except json.JSONDecodeError:
            video_paths = [p.strip() for p in raw_paths.split(',')]
    else:
        video_paths = raw_paths

    req_id = params.get('req_id', 'manual_test')
    remove_silence = params.get('remove_silence', True)
    
    valid_paths = [p for p in video_paths if os.path.exists(p)]
    if len(valid_paths) < 1:
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

    # Determine Target Resolution
    meta_first = get_video_metadata(valid_paths[0])
    target_w = meta_first['width']
    target_h = meta_first['height']
    logger.info(f"Target Resolution: {target_w}x{target_h}")

    # Process Segments (with silence removal)
    processed_clips = []
    
    for idx, raw_clip in enumerate(valid_paths):
        out_name = temp_dir / f"norm_{idx:03d}.mp4"
        duration = prepare_segment(str(raw_clip), str(out_name), target_w, target_h, 
                                   remove_silence=remove_silence)
        processed_clips.append({'path': str(out_name), 'duration': duration})

    # Iterative Merge with concatenated audio
    current_merged_path = processed_clips[0]['path']
    current_duration = processed_clips[0]['duration']

    if len(processed_clips) > 1:
        for i in range(1, len(processed_clips)):
            next_clip = processed_clips[i]
            step_output = temp_dir / f"step_merge_{i}.mp4"
            
            new_duration = merge_pair_concat_audio(
                current_merged_path, 
                next_clip['path'], 
                str(step_output), 
                current_duration
            )
            
            current_merged_path = str(step_output)
            current_duration = new_duration

    # Add Watermark (with silence removal on watermark too)
    aspect_ratio = target_w / target_h
    watermark_source = WATERMARK_PATH_16_9 if aspect_ratio > 1.0 else WATERMARK_PATH_9_16

    if os.path.exists(watermark_source):
        logger.info(f"Appending watermark: {watermark_source}")
        
        wm_norm_path = temp_dir / "norm_watermark.mp4"
        # Don't remove silence from watermark - it's branding content
        wm_duration = prepare_segment(watermark_source, str(wm_norm_path), target_w, target_h, 
                                     remove_silence=False)
        
        wm_merge_output = temp_dir / "step_merge_branding.mp4"
        new_duration = merge_pair_concat_audio(
            current_merged_path,
            str(wm_norm_path),
            str(wm_merge_output),
            current_duration
        )
        
        current_merged_path = str(wm_merge_output)
        current_duration = new_duration
    else:
        logger.warning(f"Watermark missing at {watermark_source}")

    # Final Polish
    final_output_name = f"merged_{req_id}.mp4"
    final_output_path = output_dir / final_output_name
    
    finalize_video(current_merged_path, str(final_output_path))    
    logger.info(f"✅ Successfully created: {final_output_path}")
    
    # Cleanup
    try:
        shutil.rmtree(temp_dir)
    except:
        pass

    return str(final_output_path)


# ================= DAG Definition =================

with DAG(
    dag_id='video_merge_pipeline_enhanced',
    default_args=default_args,
    description='Enhanced video merger with silence removal and smart audio handling',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['lowtouch', 'video', 'ffmpeg', 'enhanced'],
    params={
        "video_paths": Param(
            default=["/appz/shared/mock/part1.mp4", "/appz/shared/mock/part2.mp4"],
            type="array",
            description="List of video file paths to merge"
        ),
        "req_id": Param(
            default="manual_test",
            type="string",
            description="Request ID"
        ),
        "remove_silence": Param(
            default=True,
            type="boolean",
            description="Enable silence removal from segments"
        )
    }
) as dag:

    merge_task = PythonOperator(
        task_id='merge_video_files',
        python_callable=merge_videos_logic,
        provide_context=True
    )

    merge_task