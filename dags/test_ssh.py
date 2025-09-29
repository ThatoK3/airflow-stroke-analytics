from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Get the path to the DAGs folder
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
PEM_FILE = os.path.join(DAGS_FOLDER, "mlops-proj.pem")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ssh_test_file_creation',
    default_args=default_args,
    schedule_interval=None,  
    start_date=datetime(2025, 9, 28),
    catchup=False,
    tags=['test', 'ssh']
) as dag:

    create_test_file = BashOperator(
        task_id='create_test_file_via_ssh',
        bash_command=f"""
        # SSH using .pem file from DAGs folder
        ssh -i "{PEM_FILE}" \
            -o StrictHostKeyChecking=no \
            ubuntu@ec2-18-208-221-9.compute-1.amazonaws.com '
            
            # Create test file with timestamp
            TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
            TEST_FILE="/home/ubuntu/test_execution_$TIMESTAMP.txt"
            
            echo "Airflow SSH Test Successful" > $TEST_FILE
            echo "Timestamp: $(date)" >> $TEST_FILE
            echo "Hostname: $(hostname)" >> $TEST_FILE
            echo "Working Directory: $(pwd)" >> $TEST_FILE
            
            # Verify file was created
            if [ -f "$TEST_FILE" ]; then
                echo "SUCCESS: Test file created: $TEST_FILE"
                ls -la $TEST_FILE
                cat $TEST_FILE
            else
                echo "ERROR: Failed to create test file"
                exit 1
            fi
        '
        """
    )
