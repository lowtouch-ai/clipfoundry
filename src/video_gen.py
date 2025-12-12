from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.models import Variable
import logging

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

def process_config(**context):
    """Task 2: Pull config from XCom and process (sample action)."""
    ti = context['ti']
    config = ti.xcom_pull(key='config', task_ids='get_config')
    if not config:
        raise ValueError("No config found in XCom")
    
    # Sample processing logic (e.g., validate or simulate work)
    logger.info(f"Processing with config: {config}")
    # Example: Simulate image/video processing based on video_model
    video_model = config['video_model']
    if video_model == 'mock':
        logger.info("Using mock video model for simulation.")
    else:
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