
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "social_media_pipeline"
SCHEDULE = "0 * * * *"  # hourly. Use "0 0 * * *" for daily.

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

COMMON_ENV = {
    "SINCE_HOURS": "1",            
    "RATE_LIMIT_SLEEP_SEC": "3",   
    # Add DB/API env like:
    # "POSTGRES_URI": "postgresql://user:pass@host:5432/dbname",
    # "REDDIT_CLIENT_ID": "...",
    # "YOUTUBE_API_KEY": "...",
}

with DAG(
    dag_id=DAG_ID,
    description="ETL of Reddit/4chan/YouTube comments + NLP analysis and housekeeping",
    start_date=datetime(2025, 8, 1),
    schedule=SCHEDULE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["etl", "social-media", "nlp", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ---------------------
    # Extract/Load per source (parallel)
    # ---------------------
    reddit_etl = BashOperator(
        task_id="reddit_etl",
        bash_command="python Reddit.py",
        env=COMMON_ENV,
    )

    fourchan_etl = BashOperator(
        task_id="fourchan_etl",
        bash_command="python chan4.py",
        env=COMMON_ENV,
    )

    youtube_etl = BashOperator(
        task_id="youtube_etl",
        bash_command="python Youtube_final.py",
        env=COMMON_ENV,
    )

    # ---------------------
    # Unify / Normalize
    # ---------------------
    unify_normalize = BashOperator(
        task_id="unify_and_normalize",
        bash_command="python Reddit_4chan_Analysis.py",
        env=COMMON_ENV,
    )

    # ---------------------
    # NLP / Analysis
    # ---------------------
    nlp_analysis = BashOperator(
        task_id="nlp_analysis",
        bash_command="python Youtube_Analysis.py",
        env=COMMON_ENV,
    )

   
    vacuum_analyze = BashOperator(
        task_id="vacuum_analyze",
        bash_command='psql "$POSTGRES_URI" -c "VACUUM (VERBOSE, ANALYZE);"',
        env=COMMON_ENV,
        trigger_rule=TriggerRule.ALL_DONE,  # run even if previous failed
    )

    end = EmptyOperator(task_id="end")

    # Orchestration: start -> [parallel ETL] -> unify -> analysis -> housekeeping -> end
    chain(
        start,
        [reddit_etl, fourchan_etl, youtube_etl],
        unify_normalize,
        nlp_analysis,
        vacuum_analyze,
        end,
    )