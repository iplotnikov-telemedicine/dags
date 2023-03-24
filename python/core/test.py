import yaml
import os
from typing import Dict
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class MappingItem:
    source: str
    # target: str

@dataclass
class JobConfig:
    database: str
    schema: str
    table: str
    increment_column: str
    # source: TableInfo
    # target: TableInfo
    map: List[MappingItem]
    load_type: str = '_unknown_'
    pk: str = None

    def __post_init__(self):
        self.map = [MappingItem(**kv) for kv in self.map]
        # self.source = TableInfo(**self.source)
        # self.target = TableInfo(**self.target)
        if self.database is None:
            self.database = 'dev'
        if self.schema is None:
            self.schema = 'mock'




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

def get_all_job_names(directory: str):
    job_names = []
    for file in os.listdir(directory):
        if file.endswith('.yml') and "_sample" not in file:
            job_name = os.path.splitext(file)[0]
            job_names.append(job_name)
    return job_names

job_configs_dict = get_all_job_names(yml_path)
print(job_configs_dict)


def get_jobs():
    job_list = []
    for job in get_all_job_names(yml_path):
        job_list.append({'task_id': 'upsert_' + job, 'op_args': [job]})
    return job_list

print(get_jobs())