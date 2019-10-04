# start up
airflow webserver -p 8081
airflow scheduler
# test dag
python ~/airflow/dags/data_airflow.py
# print the list of active DAGs
airflow list_dags

# prints the list of tasks the "tutorial" dag_id
airflow list_tasks data_airflow

# prints the hierarchy of tasks in the tutorial DAG
airflow list_tasks data_airflow --tree

# command layout: command subcommand dag_id task_id date

# testing print_date
airflow test data_airflow download_unzip_data 2015-06-01

# testing sleep
airflow test data_airflow run_spark 2015-06-01