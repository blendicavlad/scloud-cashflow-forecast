from typing import Hashable, Any, Union, Dict

import yaml


def read_yaml(filename: str) -> Union[Dict[Hashable, Any], list, None]:
    with open(filename, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise 'Unable to read YAML file'
