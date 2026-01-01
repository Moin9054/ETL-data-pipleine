import sys
sys.path.insert(0, "/opt/airflow/scripts")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from extract import extract_popular_movies, extract_top_rated_movies, extract_trending_movies, extract_genres
from transform import transform_movies, generate_stats
from load import load_movies, load_genres, load_daily_stats

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "movie_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for TMDB movie data",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["movies", "etl", "tmdb"],
)

def _extract_popular(**context):
    filepath = extract_popular_movies()
    context["ti"].xcom_push(key="popular_path", value=filepath)
    return filepath

def _extract_top_rated(**context):
    filepath = extract_top_rated_movies()
    context["ti"].xcom_push(key="top_rated_path", value=filepath)
    return filepath

def _extract_trending(**context):
    filepath = extract_trending_movies()
    context["ti"].xcom_push(key="trending_path", value=filepath)
    return filepath

def _extract_genres(**context):
    filepath = extract_genres()
    context["ti"].xcom_push(key="genres_path", value=filepath)
    return filepath

def _transform_movies(**context):
    ti = context["ti"]
    popular_path = ti.xcom_pull(task_ids="extract_popular_movies", key="popular_path")
    top_rated_path = ti.xcom_pull(task_ids="extract_top_rated_movies", key="top_rated_path")
    trending_path = ti.xcom_pull(task_ids="extract_trending_movies", key="trending_path")
    genres_path = ti.xcom_pull(task_ids="extract_genres", key="genres_path")
    
    result = transform_movies(popular_path, top_rated_path, trending_path, genres_path)
    ti.xcom_push(key="movies_csv_path", value=result["movies_path"])
    ti.xcom_push(key="genres_csv_path", value=result["genres_path"])
    return result

def _generate_stats(**context):
    ti = context["ti"]
    movies_path = ti.xcom_pull(task_ids="transform_movies", key="movies_csv_path")
    stats_path = generate_stats(movies_path)
    ti.xcom_push(key="stats_path", value=stats_path)
    return stats_path

def _load_movies(**context):
    ti = context["ti"]
    movies_path = ti.xcom_pull(task_ids="transform_movies", key="movies_csv_path")
    count = load_movies(movies_path)
    return f"Loaded {count} movies"

def _load_genres(**context):
    ti = context["ti"]
    genres_path = ti.xcom_pull(task_ids="transform_movies", key="genres_csv_path")
    count = load_genres(genres_path)
    return f"Loaded {count} genres"

def _load_stats(**context):
    ti = context["ti"]
    stats_path = ti.xcom_pull(task_ids="generate_stats", key="stats_path")
    load_daily_stats(stats_path)
    return "Stats loaded successfully"

extract_popular_task = PythonOperator(
    task_id="extract_popular_movies",
    python_callable=_extract_popular,
    dag=dag,
)

extract_top_rated_task = PythonOperator(
    task_id="extract_top_rated_movies",
    python_callable=_extract_top_rated,
    dag=dag,
)

extract_trending_task = PythonOperator(
    task_id="extract_trending_movies",
    python_callable=_extract_trending,
    dag=dag,
)

extract_genres_task = PythonOperator(
    task_id="extract_genres",
    python_callable=_extract_genres,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_movies",
    python_callable=_transform_movies,
    dag=dag,
)

generate_stats_task = PythonOperator(
    task_id="generate_stats",
    python_callable=_generate_stats,
    dag=dag,
)

load_movies_task = PythonOperator(
    task_id="load_movies",
    python_callable=_load_movies,
    dag=dag,
)

load_genres_task = PythonOperator(
    task_id="load_genres",
    python_callable=_load_genres,
    dag=dag,
)

load_stats_task = PythonOperator(
    task_id="load_stats",
    python_callable=_load_stats,
    dag=dag,
)

[extract_popular_task, extract_top_rated_task, extract_trending_task, extract_genres_task] >> transform_task
transform_task >> generate_stats_task
transform_task >> [load_movies_task, load_genres_task]
generate_stats_task >> load_stats_task
