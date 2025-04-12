from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime

from cosmos import (
    DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup, 
    DbtRunLocalOperator, RenderConfig
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Cấu hình DBT
DBT_PROJECT_DIR = "/opt/airflow/dbt_project/thaidh_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"

profile_config = ProfileConfig(
    profile_name="thaidh_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dbt_postgres_conn_1",
        profile_args={"schema": "public"},
    ),
)

# Khai báo DAG
with DAG(
    dag_id="dbt_thai_cosmos_postgres",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ✅ Task 1: Kiểm tra kết nối DBT
    check_connection = DbtRunLocalOperator(
        task_id="check_dbt_connection",
        dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        profile_config=profile_config,  # ✅ Thêm profile_config để tránh lỗi
        commands=["debug"],  # Chạy dbt debug để kiểm tra kết nối
    )

    # ✅ Task 2: Cài đặt dependencies với dbt deps
    dbt_deps = DbtRunLocalOperator(
        task_id="install_dbt_dependencies",
        dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        profile_config=profile_config,  # ✅ Thêm profile_config
        commands=["deps"],  # Chạy dbt deps để cài đặt dependencies
    )

    # ✅ Task 3: DBT Task Group để quản lý luồng DBT
    dbt_task_group = DbtTaskGroup(
        group_id="dbt_tasks_1",
        execution_config=ExecutionConfig(
            dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"
        ),
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
        ),
        profile_config=profile_config,
        render_config=RenderConfig( 
            select=["tag:dbt"],
        )
    )

    # 🔗 Thiết lập thứ tự chạy: 
    check_connection >> dbt_deps >> dbt_task_group
