import pendulum

from airflow.decorators import dag, task

default_args = {}


@dag(dag_id='AA_TaskflowApi',
     default_args=default_args,
     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
     schedule=None)
def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
        nicknames = ["Nickname1", "Nickname2"]
        return {
            'name': 'Jerry',
            'nicks': nicknames
        }

    @task
    def print_nicks(nick):
        print(f"Processing nickname {nick}")

    @task
    def get_nicks(name_dict):
        return  name_dict['nicks']

    name_dict = get_name()
    nicks = get_nicks(name_dict)
    print(f'nicks = {nicks}')

    print_nicks.expand(nick=nicks)


greet_dag = hello_world_etl()