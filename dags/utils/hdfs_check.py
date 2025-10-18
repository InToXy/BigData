from airflow.operators.bash import BashOperator

def create_hdfs_check_task(dag):
    """Crée une tâche pour vérifier la connexion HDFS"""
    return BashOperator(
        task_id='check_hdfs_connection',
        bash_command='hdfs dfs -ls / || exit 1',
        dag=dag
    )