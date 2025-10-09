from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import os
import subprocess

# -------- Configuration --------
JUPYTER_URL = "http://hostxxxx:9999"
NOTEBOOK_PATH = "work/spark_analytics.ipynb"
JUPYTER_TOKEN = "token"

# SSH Configuration
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
PEM_FILE = os.path.join(DAGS_FOLDER, "xxx.pem")
SSH_SERVER = "ubuntu@ec2-18xxxxxxxx.compute-1.amazonaws.com"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True,
    'execution_timeout': timedelta(minutes=15)
}

def check_jupyter_health():
    """Check Jupyter server health via API"""
    try:
        response = requests.get(
            f"{JUPYTER_URL}/api",
            headers={"Authorization": f"token {JUPYTER_TOKEN}"},
            timeout=30
        )
        response.raise_for_status()

        # Check notebook exists
        nb_response = requests.get(
            f"{JUPYTER_URL}/api/contents/{NOTEBOOK_PATH}",
            headers={"Authorization": f"token {JUPYTER_TOKEN}"},
            timeout=30
        )

        if nb_response.status_code == 404:
            raise Exception(f"Notebook not found: {NOTEBOOK_PATH}")
        nb_response.raise_for_status()

        print(f"Jupyter API healthy, notebook exists: {NOTEBOOK_PATH}")
        return True

    except Exception as e:
        print(f"Jupyter API health check failed: {str(e)}")
        return False

def check_ssh_health():
    """Check SSH connectivity and container health"""
    try:
        # Test SSH connection and container status
        result = subprocess.run([
            'ssh', '-i', PEM_FILE, '-o', 'StrictHostKeyChecking=no',
            '-o', 'ConnectTimeout=10', SSH_SERVER,
            'docker ps | grep pyspark && echo "SSH and container healthy"'
        ], capture_output=True, text=True, timeout=30)

        if result.returncode == 0 and "pyspark" in result.stdout:
            print("SSH and container healthy")
            return True
        else:
            print(f"SSH health check failed: {result.stderr}")
            return False

    except Exception as e:
        print(f"SSH health check failed: {str(e)}")
        return False

def cleanup_resources():
    """Clean up stale kernels and temp files"""
    try:
        # Clean kernels via API
        response = requests.get(
            f"{JUPYTER_URL}/api/kernels",
            headers={"Authorization": f"token {JUPYTER_TOKEN}"},
            timeout=30
        )
        response.raise_for_status()  # Raise exception for bad status
        kernels = response.json()

        for kernel in kernels:
            kernel_id = kernel['id']
            if 'last_activity' in kernel:
                last_activity = datetime.fromisoformat(kernel['last_activity'].replace('Z', '+00:00'))
                if datetime.now(last_activity.tzinfo) - last_activity > timedelta(minutes=30):
                    try:
                        requests.delete(
                            f"{JUPYTER_URL}/api/kernels/{kernel_id}",
                            headers={"Authorization": f"token {JUPYTER_TOKEN}"},
                            timeout=10
                        )
                        print(f"Cleaned up kernel: {kernel_id}")
                    except Exception:
                        pass

        # Clean temp files via SSH
        subprocess.run([
            'ssh', '-i', PEM_FILE, '-o', 'StrictHostKeyChecking=no',
            SSH_SERVER,
            'docker exec pyspark find /tmp -name "spark-*" -type d -mmin +60 -exec rm -rf {} + 2>/dev/null || true'
        ], capture_output=True, text=True, timeout=30)

        print("Resource cleanup completed")
        return True

    except Exception as e:
        print(f"Cleanup warning: {str(e)}")
        return True

def execute_via_api():
    """Try executing notebook via Jupyter API first"""
    try:
        print("Attempting API execution...")
        url = f"{JUPYTER_URL}/api/contents/{NOTEBOOK_PATH}/execute"
        headers = {
            "Authorization": f"token {JUPYTER_TOKEN}",
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, json={}, timeout=300)

        if response.status_code == 200:
            print("Notebook executed successfully via API")
            return True
        else:
            print(f"API execution failed (status {response.status_code}), falling back to SSH...")
            return False

    except Exception as e:
        print(f"API execution failed: {str(e)}, falling back to SSH...")
        return False

with DAG(
    dag_id='spark_notebook_combined',
    default_args=default_args,
    schedule='0 * * * *',
    start_date=datetime(2025, 9, 28),
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'notebook', 'hourly', 'combined']
) as dag:

    # Health checks (both API and SSH)
    api_health_check = PythonOperator(
        task_id='api_health_check',
        python_callable=check_jupyter_health
    )

    ssh_health_check = PythonOperator(
        task_id='ssh_health_check',
        python_callable=check_ssh_health
    )

    # Cleanup
    cleanup = PythonOperator(
        task_id='cleanup_resources',
        python_callable=cleanup_resources
    )

    # Primary execution method - API
    execute_api = PythonOperator(
        task_id='execute_via_api',
        python_callable=execute_via_api
    )

    # Fallback execution method - SSH (runs only if API fails)
    execute_ssh = BashOperator(
        task_id='execute_via_ssh',
        bash_command=f'''
        ssh -i "{PEM_FILE}" \
            -o StrictHostKeyChecking=no \
            -o ConnectTimeout=30 \
            {SSH_SERVER} '

            echo "Starting notebook execution via SSH at $(date)"

            # Execute notebook in container
            if docker exec pyspark jupyter execute /home/jovyan/work/spark_analytics.ipynb; then
                echo "Notebook executed successfully via SSH"
                exit 0
            else
                echo "SSH execution failed"
                exit 1
            fi
        '
        ''',
        trigger_rule='one_failed'  # Run only if API execution fails
    )

    # Verification
    verify_execution = BashOperator(
        task_id='verify_execution',
        bash_command=f'''
        # Verify via API that notebook is accessible
        curl -s -H "Authorization: token {JUPYTER_TOKEN}" \
            "{JUPYTER_URL}/api/contents/{NOTEBOOK_PATH}" > /dev/null && \
        echo "Notebook verification passed" || \
        echo "Notebook verification warning"
        ''',
        trigger_rule='all_done'
    )

    # Final log
    final_log = BashOperator(
        task_id='final_log',
        bash_command='echo "Notebook execution pipeline completed at $(date)"',
        trigger_rule='all_done'
    )

    # Set up task dependencies
    [api_health_check, ssh_health_check] >> cleanup
    cleanup >> execute_api
    execute_api >> [execute_ssh, verify_execution]  # execute_ssh runs only if execute_api fails
    [execute_api, execute_ssh] >> verify_execution
    verify_execution >> final_log
