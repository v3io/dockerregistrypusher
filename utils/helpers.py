import hashlib
import json


def get_digest(filepath):
    return "sha256:" + get_file_sha256(filepath)


def load_json_file(filepath):
    with open(filepath, 'r') as fh:
        return json.loads(fh.read())


def dump_json_file(filepath, json_contents):
    with open(filepath, 'w') as fh:
        json.dump(json_contents, fh)


def get_file_sha256(filepath):
    sha256hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            data = f.read(65536)
            sha256hash.update(data)
            if not data:
                break
    return sha256hash.hexdigest()
