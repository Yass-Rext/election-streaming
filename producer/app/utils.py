import json
import os


DATA_PATH = "/app/data"


def load_json(filename):

    path = os.path.join(DATA_PATH, filename)

    print(f"Chargement du fichier : {path}")

    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)