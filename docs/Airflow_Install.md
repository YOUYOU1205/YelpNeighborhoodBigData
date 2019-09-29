{\rtf1\ansi\ansicpg1252\cocoartf1671\cocoasubrtf500
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww10800\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 ### Install Apache Airflow\
- add the following lines to `~/.bashrc` and source it\
    ```\
    export AIRFLOW_HOME=~/airflow\
    export SLUGIFY_USES_TEXT_UNIDECODE=yes  (optional, if there is a dependency issue)\
    ```\
- use `pip` to install Airflow\
    ```\
    pip install apache-airflow --user\
    ```\
  (add '--user' if there is a permission issue)\
- check installation\
    ```\
    airflow version\
    ```\
- change the default timezone in `airflow.cfg` (optional)\
    ```\
    default_timezone = est\
    ```\
\
\
### Run Airflow DAGs\
- move the python script to `$AIRFLOW_HOME/dags/`\
- execute the python script\
    ```\
    python example.py\
    ```\
- initialize databases\
    ```\
    airflow initdb\
    ```\
- launch webserver\
    ```\
    airflow webserver -p 8081\
    ```\
- open another terminal; use following command to launch scheduler\
    ```\
    airflow scheduler\
    ```\
- go to the webUI, `http://<EC2 public DNS>:8081`\
- enable the dag (on the left) and then execute it on the most left button on the small menu with the __'play\'92__ icon\
}