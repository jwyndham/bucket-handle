import json


def read_json(fp: str):
    with open(fp, 'r') as f:
        file = json.load(f)
    return file


def write_json(obj: dict, fp: str):
    with open(fp, 'w') as f:
        json.dump(obj, f, indent=2)
