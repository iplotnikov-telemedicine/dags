import yaml
from python.core.datamodels import JobConfig

yml_path = '/Users/a.bezgodov/airflow/dags/yml/'


def get_job_config(job) -> JobConfig:
    with open(yml_path + job + '.yml', 'r') as jc:
        conf_yaml = yaml.safe_load(jc)
    return JobConfig(**conf_yaml)