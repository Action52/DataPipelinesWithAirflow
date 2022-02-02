export AIRFLOW_HOME="~/airflow/"
airflow scheduler --daemon
airflow webserver --daemon -p 3000
