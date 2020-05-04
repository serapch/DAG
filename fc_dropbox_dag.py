from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# model modules
import os
import numpy as np
import pandas as pd
from ca_client.api_client import *
from esim.Utils.SimulationItemsBuilder import *
from esim.DataSource.Csv import Csv
from esim.DataSource.DropBoxDataSource import DropBoxDataSource
from esim.SimulationController.SimulationControllerFactory import SimulationControllerFactory


def get_request_file_data():
    request_local_path = '/usr/local/airflow/dags/sample_request_spain_pv_FC_DropBoxShorter.json'
    with open(request_local_path, 'r') as fh:
        data = fh.read()
    return data

def deserialize_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_request_file_data')
    client = ApiClient()
    return client.deserialize_request(data)
    
def run_model(**kwargs):
    os.chdir(os.getcwd() + "/dags") # TODO change cwd to the proper one
    ti = kwargs['ti']
    deserialized_request = ti.xcom_pull(task_ids='deserialize_data')
    model_config = deserialized_request.model_configuration
    termsheet = deserialized_request.termsheet
    pricing_spec = deserialized_request.pricing_spec

    termsheet_items = get_sparse_simulation_items_from_termsheet(deserialized_request, market_name='SPAIN_BASE')
    model_id = model_config.model_id

    drop_box_data_source = DropBoxDataSource(deserialized_request.drop_box)

    # SnowFlake
    sim_controller = SimulationControllerFactory(model_id,
                                                 Csv()).power_pv(termsheet_items,
                                                                       np.array(model_config.
                                                                                spot_model_correlation).
                                                                       reshape(2, 2),
                                                                       drop_box_data_source)

    result = sim_controller.run(
        eod=pricing_spec.as_of_date,
        end_simulation=termsheet.delivery_period.end,
        # termsheet delivery end date + 1 month for esim mean reversion
        nbr_mc_scenarios=model_config.n_sims,
        seed=model_config.seed
    )

    ref = deserialized_request.drop_box.forward_curves["POWER_ESP_FC.MID"].values[0][1]
    res = result[1].HourlyFwdCurves[0].values[0]

    #### Check that hourly Sims are calibrated to dropBox hfC, we do have DST change in here:#########################

    ref_hourly = np.mean(result[1].HourlySimulations[0], axis=1)

    ref2_hourly = pd.DataFrame(deserialized_request.drop_box.forward_curves["POWER_ESP_FC.MID"].values)[1]
    ref2_hourly = np.array(ref2_hourly)
    delta = (termsheet.delivery_period.end - pricing_spec.as_of_date).days
    end_hourly = delta * 24
    ref2_hourly = ref2_hourly[24:end_hourly]

    avgHourlyDiffs = np.mean(ref_hourly - ref2_hourly)
    
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'fc_dropbox_dag',
    default_args=default_args,
    description='First dag',
    schedule_interval=timedelta(days=1),
)

### tasks ###
read_file_task = PythonOperator(
        task_id='get_request_file_data',
        python_callable=get_request_file_data,
        dag=dag,
)


deserialize_data_task = PythonOperator(
        task_id='deserialize_data',
        python_callable=deserialize_data,
        provide_context=True,
        dag=dag,
)

run_model_task = PythonOperator(
        task_id='run_model',
        python_callable=run_model,
        provide_context=True,
        dag=dag,
)

read_file_task >> deserialize_data_task >> run_model_task
