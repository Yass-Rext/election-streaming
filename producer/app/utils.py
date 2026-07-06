import json
import os

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config"))

def load_json(filename):
    path = os.path.join(BASE_PATH, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)