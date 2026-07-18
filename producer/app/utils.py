import json
import os

DATA_PATH = "/app/data"


def load_json(filename: str):

    path = os.path.join(DATA_PATH, filename)

    print(f"Chargement : {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)