import yaml
import os
from python.core.datamodels import JobConfig

current_dir = os.getcwd()
current_dir_name = os.path.basename(current_dir)
if current_dir_name == 'airflow':
    yml_path = os.path.join(current_dir, 'dags', 'yml')
elif current_dir_name == 'dags':
    yml_path = os.path.join(current_dir, 'yml')


def get_job_config(job) -> JobConfig:
    with open(yml_path + '/' + job + '.yml', 'r') as jc:
        conf_yaml = yaml.safe_load(jc)
    return JobConfig(**conf_yaml)


def get_all_job_names(path = yml_path):
    job_names = []
    for file in os.listdir(path):
        if file.endswith('.yml') and "_sample" not in file:
            job_name = os.path.splitext(file)[0]
            job_names.append(job_name)
    return job_names