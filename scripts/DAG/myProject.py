#====================================================================================================================================#
#                                                                                                                                    #
#                 *************************************************************************************************                  #
#                 *           This Is Airflow Dag, used to run and schedule pySpark scripts in sequence           *                  #
#                 *************************************************************************************************                  #
#                                                                                                                                    #
#             Script Name  = myProject.py                                                                                            #
#             Description  = This script will run pyspark scripts in order and then mail the status of scripts (success              #
#                            or failed) and then mail logs to provided mail id.                                                      #
#             Arguments    = None                                                                                                    #
#             Dependencies = None                                                                                                    #
#             Author       = Ayush Sharma                                                                                            #
#             Email        = myproject.dea@gmail.com                                                                                 #
#             Date         = 18-04-2025 (dd-mm-yyyy format)                                                                          #
#                                                                                                                                    #
#                                                                                                                                    #
#====================================================================================================================================#


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.models import DagRun
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'email': ['myproject.dea@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False
}

dag = DAG(
    'myProject',
    default_args=default_args,
    description='Run PySpark scripts for cricket data in sequence',
    schedule_interval=None,
    catchup=False
)

# -------- FAILURE EMAIL WITH LOG ATTACHMENTS --------
def send_failure_email(**context):
    dag_run = context.get("dag_run")
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y-%m-%dT%H:%M:%S')  # Match Airflow timestamp format

    failed_tasks = [ti for ti in dag_run.get_task_instances() if ti.state == State.FAILED]
    if not failed_tasks:
        return

    attached_logs = []
    for ti in failed_tasks:
        try_number = ti.try_number if ti.try_number > 0 else 1
        execution_ts = execution_date.isoformat()
        log_path = f"/home/hadoop/airflow/logs/{dag_id}_{ti.task_id}_{execution_ts}.log"

        if os.path.exists(log_path):
            attached_logs.append(log_path)
        else:
            tmp_log = f"/tmp/{ti.task_id}_log_missing.txt"
            with open(tmp_log, 'w') as f:
                f.write(f"Log file not found for {ti.task_id} at expected path: {log_path}")
            attached_logs.append(tmp_log)


    html_content = f"""
    <h3>Airflow DAG Failed</h3>
    <p><b>DAG:</b> {dag_id}<br>
    <b>Run ID:</b> {run_id}<br>
    <b>Execution Date:</b> {execution_date}</p>
    <p>Failed task logs are attached to this email.</p>
    """

    send_email(
        to=["myproject.dea@gmail.com"],
        subject=f"[Failure] Airflow DAG: {dag_id}",
        html_content=html_content,
        files=attached_logs
    )


# -------- SUCCESS EMAIL --------
def send_success_email(**context):
    dag_run = context.get("dag_run")
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date']
    timestamp = execution_date.strftime('%Y-%m-%dT%H:%M:%S')

    attached_logs = []
    for ti in dag_run.get_task_instances():
        if ti.state == State.SUCCESS:
            try_number = ti.try_number if ti.try_number > 0 else 1
            execution_ts = execution_date.isoformat()
            log_path = f"/home/hadoop/airflow/logs/{dag_id}_{ti.task_id}_{execution_ts}.log"
            if os.path.exists(log_path):
                attached_logs.append(log_path)

    html_content = f"""
    <h3>Airflow DAG Succeeded</h3>
    <p><b>DAG:</b> {dag_id}<br>
    <b>Run ID:</b> {run_id}<br>
    <b>Execution Date:</b> {execution_date}</p>
    <p>All tasks completed successfully ✅</p>
    """

    send_email(
        to=["myproject.dea@gmail.com"],
        subject=f"[Success] Airflow DAG: {dag_id}",
        html_content=html_content,
        files=attached_logs
    )

# -------- TASKS --------
matches_task = BashOperator(
    task_id='run_matches',
    bash_command='spark-submit /home/hadoop/scripts/matches.py',
    dag=dag
)

deliveries_task = BashOperator(
    task_id='run_deliveries',
    bash_command='spark-submit /home/hadoop/scripts/deliveries.py',
    dag=dag
)

players_task = BashOperator(
    task_id='run_players',
    bash_command='spark-submit /home/hadoop/scripts/players.py',
    dag=dag
)

# -------- FINAL SUCCESS EMAIL TASK --------
final_success_email = PythonOperator(
    task_id='send_success_email',
    python_callable=send_success_email,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,  # Only when all previous tasks succeed
    dag=dag
)

# -------- FINAL FAILURE EMAIL TASK --------
final_failure_email = PythonOperator(
    task_id='send_failure_email',
    python_callable=send_failure_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,  # Only when any previous task fails
    dag=dag
)

# -------- DEPENDENCIES --------
[matches_task, deliveries_task, players_task] >> final_success_email
[matches_task, deliveries_task, players_task] >> final_failure_email
