import yaml
import os
from filelock import FileLock

CONFIG_PATH = "config.yaml"
LOCK_PATH = CONFIG_PATH + ".lock"

def retrieve_yaml_file():
    config = {}
    try:
        with FileLock(LOCK_PATH):
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r') as file:
                    config = yaml.safe_load(file) or {}
    except Exception as e:
        print(f"⚠️ Error reading YAML file: {e}")

    return config