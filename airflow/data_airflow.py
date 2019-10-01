
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 1),
    'email': ['carrieliu611@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('data_airflow', default_args=default_args,
          schedule_interval=timedelta(days=1))

download_unzip_data_command = """
    echo "Start downloading data..."
    curl 'https://www.southernnevadahealthdistrict.org/restaurants/download/restaurants.zip' -o /home/ubuntu/data_{{  ds  }}.zip
    echo "finish downloading"
    echo "Start unziping..."
    mkdir /home/ubuntu/data_{{  ds  }}
    unzip /home/ubuntu/data_{{  ds  }}.zip -d /home/ubuntu/data_{{  ds  }}
    hdfs dfs -copyFromLocal /home/ubuntu/data_{{  ds  }}/restaurant_establishments.csv /user/restaurant_establishments_{{  ds  }}.csv
    echo "Done!"
    """

t1 = BashOperator(
    task_id='download_unzip_data',
    bash_command=download_unzip_data_command,
    dag=dag)

run_spark_command = """
    spark-submit  --packages org.postgresql:postgresql:9.4.1207 --master spark://ec2-35-160-13-109.us-west-2.compute.amazonaws.com:7077  /home/ubuntu/s3/data_process.py 'hdfs://ec2-35-160-13-109.us-west-2.compute.amazonaws.com:9000/user/restaurant_establishments_{{  ds  }}.csv'
    echo "Done!"
    """

t2 = BashOperator(
    task_id='run_spark',
    bash_command=run_spark_command,
    retries=1,
    dag=dag)

t2.set_upstream(t1)
