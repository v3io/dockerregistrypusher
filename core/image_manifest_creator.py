import json


class ImageManifestCreator(object):
    def __init__(self, name, tag, config_path, layers_info, config_info):
        self._name = name
        self._tag = tag
        self._config_path = config_path
        self._layers_info = layers_info
        self._config_info = config_info

    def create(self):
        manifest = dict()
        manifest["schemaVersion"] = 2
        manifest["mediaType"] = "application/vnd.docker.distribution.manifest.v2+json"
        manifest["config"] = {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "size": self._config_info['size'],
            "digest": self._config_info['digest'],
        }
        manifest["layers"] = []
        for layer_info in self._layers_info:
            if layer_info['ext'].endswith('gz') or layer_info['ext'].endswith('gzip'):
                media_type = "application/vnd.docker.image.rootfs.diff.tar.gzip"
            else:
                media_type = "application/vnd.docker.image.rootfs.diff.tar.gzip"
            layer_data = {
                "mediaType": media_type,
                "size": layer_info['size'],
                "digest": layer_info['digest'],
            }
            manifest["layers"].append(layer_data)

        return json.dumps(manifest)
