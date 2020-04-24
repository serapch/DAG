import airflow
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
   'owner': 'airflow',
   'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
   dag_id='kubernetes-operator-failing',
   default_args=default_args,
   schedule_interval=None
)
message = Variable.get("message", deserialize_json=True)
printMessage = message["text1"]
#message = Variable.get("message", deserialize_json=True)

task1 = KubernetesPodOperator(namespace='default',
                          image="python:3.6",
                          cmds=["python","-c"],
                          arguments=["print('Hello Hell')"],
                          labels={"foo": "bar"},
                          name="failing-test",
                          task_id="failing-task",
						  email='rodislav.rusev@scalefocus.com',
						  email_on_failure=False,
                          get_logs=True,
                          dag=dag
)
