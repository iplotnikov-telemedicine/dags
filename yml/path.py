import os


def get_yml_path():
    full_path = os.path.realpath(__file__)
    yml_dir_path = os.path.dirname(full_path)
    return yml_dir_path