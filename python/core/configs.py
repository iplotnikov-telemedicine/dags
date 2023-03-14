import yaml
from python.core.datamodels import JobConfig
from yml.path import get_yml_path


yml_path = get_yml_path()


def get_job_config(job) -> JobConfig:
    with open(yml_path + '/' + job + '.yml', 'r') as jc:
        conf_yaml = yaml.safe_load(jc)
    return JobConfig(**conf_yaml)
