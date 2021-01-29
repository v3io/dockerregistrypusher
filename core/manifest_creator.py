import os
import hashlib
import json


class ImageManifestCreator(object):
    def __init__(self, config_path, layers_paths):
        self._config_path = config_path
        self._layers_paths = layers_paths

    def create(self):
        manifest = dict()
        manifest["schemaVersion"] = 2
        manifest["mediaType"] = "application/vnd.docker.distribution.manifest.v2+json"
        manifest["config"] = dict()
        manifest["config"][
            "mediaType"
        ] = "application/vnd.docker.container.image.v1+json"
        manifest["config"]["size"] = os.path.getsize(self._config_path)
        manifest["config"]["digest"] = self._get_digest(self._config_path)
        manifest["layers"] = []
        for layer in self._layers_paths:
            layer_data = dict()
            layer_data["mediaType"] = "application/vnd.docker.image.rootfs.diff.tar"
            layer_data["size"] = os.path.getsize(layer)
            layer_data["digest"] = self._get_digest(layer)
            manifest["layers"].append(layer_data)

        return json.dumps(manifest)

    def _get_digest(self, filepath):
        return "sha256:" + self.get_file_sha256(filepath)

    @staticmethod
    def get_file_sha256(filepath):
        sha256hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            while True:
                data = f.read(65536)
                sha256hash.update(data)
                if not data:
                    break
        return sha256hash.hexdigest()
