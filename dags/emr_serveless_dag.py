from airflow.decorators import dag, task_group, task
from datetime import datetime
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from commons.emr_sensor import EmrServerlessJobFailureSensor

APPLICATION_ID = '00fsl7brt1mts70h' 
EXECUTION_ROLE_ARN = 'arn:aws:iam::676206924820:role/emrserverlessRole'
S3_JOBFILE_LOCATION = 's3://emr-project-raw/serverless_example/spark_processor.py'
S3_PYFILES_LOCATION = 's3://emr-project-raw/serverless_example/spark_job.zip'
ICEBERG_WAREHOUSE = 's3://emr-project-raw/serverless_example/iceberg/'
LOG_S3_LOCATION = 's3://emr-project-raw/serverless_example/logs/'

default_args = {
    'owner':'oamen',
    'start_date': datetime(2025, 5, 20)
}

@dag(
    dag_id = 'emr_serverless_dag',
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
    tags = ['emr', 'serverless', 'spark', 'iceberg'],
)
def emr_serverless_dag():

    @task_group(group_id='preprocess')
    def preprocessor():

        tasks = []

        for i in range(2):

            task_job = EmrServerlessStartJobOperator(
                task_id = f'processor_{i}',
                application_id=APPLICATION_ID,
                execution_role_arn=EXECUTION_ROLE_ARN,
                do_xcom_push=True,
                job_driver = {
                    "sparkSubmit": {
                    "entryPoint": S3_JOBFILE_LOCATION,
                    "entryPointArguments": ["--task", "task1"],
                    "sparkSubmitParameters": (
                        f"--conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog "
                        f"--conf spark.sql.catalog.my_catalog.type=hadoop "
                        f"--conf spark.sql.catalog.my_catalog.warehouse={ICEBERG_WAREHOUSE} "
                        f"--py-files {S3_PYFILES_LOCATION} "
                        ),
                        }},
                configuration_overrides={
                    "monitoringConfiguration": {
                        "s3MonitoringConfiguration": {
                            "logUri": LOG_S3_LOCATION,
                        }
                    }
                },
            )

            tasks.append(task_job.output)
        return tasks
    
    preprocessor_tasks = preprocessor()

        
    @task_group(group_id='wait_for_job')
    def wait_for_job(preprocessor_tasks):

        for index, jobrun in enumerate(preprocessor_tasks):
            EmrServerlessJobFailureSensor(
                    task_id = f'wait_for_job_{index}',
                    application_id=APPLICATION_ID,
                    job_run_id = jobrun
                )
     
    
    wait_for_job(preprocessor_tasks)

emr_serverless_dag()