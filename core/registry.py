import os
import os.path
import re
import json
import hashlib
import urllib.parse
import time
import threading
import shlex
import subprocess

import humanfriendly
import requests
import requests.auth

from . import manifest_creator


class LayersLock:
    def __init__(self):
        self._global_lock = threading.Lock()
        self._layers_locks = {}

    def get_lock(self, key):

        # global lock to check if layer was done
        self._global_lock.acquire()
        try:
            self._layers_locks.setdefault(key, threading.Lock())
            return self._layers_locks[key]
        finally:
            self._global_lock.release()


class Registry:
    def __init__(
        self,
        logger,
        registry_url,
        stream=False,
        login=None,
        password=None,
        ssl_verify=True,
        replace_tags_match=None,
        replace_tags_target=None,
    ):
        self._logger = logger.get_child('registry')

        # enrich http suffix if missing
        if urllib.parse.urlparse(registry_url).scheme not in ['http', 'https']:
            registry_url = 'http://' + registry_url
        self._registry_url = registry_url
        self._login = login
        self._password = password
        self._basicauth = None
        self._stream = stream
        self._ssl_verify = ssl_verify
        self._replace_tags_match = replace_tags_match
        self._replace_tags_target = replace_tags_target
        if self._login:
            self._basicauth = requests.auth.HTTPBasicAuth(self._login, self._password)

        self._layers_lock = LayersLock()
        self._layers_info = {}

        self._logger.debug(
            'Initialized',
            registry_url=self._registry_url,
            login=self._login,
            password=self._password,
            ssl_verify=self._ssl_verify,
            stream=self._stream,
            replace_tags_match=self._replace_tags_match,
            replace_tags_target=self._replace_tags_target,
        )

    def process_image(self, tmp_dir_name, image_config):
        """
        Processing a single image entry from extracted files - pushing to registry
        """
        repo_tags = image_config["RepoTags"]
        config_filename = image_config["Config"]
        config_path = os.path.join(tmp_dir_name, config_filename)

        self._logger.info('Processing image', repo_tags=repo_tags)
        image_start_time = time.time()
        config_parsed = self._load_json_file(config_path)

        # warning - spammy
        self._logger.verbose('Parsed image config', config_parsed=config_parsed)

        for repo in repo_tags:
            repo_tag_start_time = time.time()
            image, tag = self._parse_image_tag(repo)
            self._logger.info(
                'Pushing image repo and tag',
                image=image,
                tag=tag,
                tmp_dir_name=tmp_dir_name,
            )

            # push individual image layers
            layers = image_config["Layers"]
            manifest_layer_info = []
            for layer in layers:
                layer_digest, layer_size = self._process_layer(
                    layer, image, tmp_dir_name
                )
                manifest_layer_info.append(
                    {
                        'digest': layer_digest,
                        'size': layer_size,
                    }
                )

            # then, push image config
            self._logger.info(
                'Pushing image config', image=image, config_loc=config_filename
            )
            push_url = self._initialize_push(image)
            self._push_config(config_path, push_url)

            # Now we need to create and push a manifest for the image
            creator = manifest_creator.ImageManifestCreator(
                config_path, manifest_layer_info
            )
            image_manifest = creator.create()

            # Override tags if needed: from --replace-tags-match and --replace-tags-target
            tag = self._replace_tag(image, tag)

            self._logger.info('Pushing image tag manifest', image=image, tag=tag)
            self._push_manifest(image_manifest, image, tag)
            repo_tag_elapsed = time.time() - repo_tag_start_time
            self._logger.info(
                'Image tag Pushed',
                image=image,
                tag=tag,
                elapsed=humanfriendly.format_timespan(repo_tag_elapsed),
            )

        image_elapsed = time.time() - image_start_time
        self._logger.info(
            'Image pushed',
            repo_tags=repo_tags,
            elapsed=humanfriendly.format_timespan(image_elapsed),
        )

    def _conditional_print(self, what, end=None):
        if self._stream:
            if end:
                print(what, end=end)
            else:
                print(what)

    def _push_manifest(self, manifest, image, tag):
        headers = {
            "Content-Type": "application/vnd.docker.distribution.manifest.v2+json"
        }
        url = f'{self._registry_url}/v2/{image}/manifests/{tag}'
        response = requests.put(
            url,
            headers=headers,
            data=manifest,
            auth=self._basicauth,
            verify=self._ssl_verify,
        )
        if response.status_code != 201:
            self._logger.log_and_raise(
                'error',
                'Failed to push manifest',
                manifest=manifest,
                image=image,
                tag=tag,
                status_code=response.status_code,
                content=response.content,
            )

    def _process_layer(self, layer, image, tmp_dir_name):

        # isolate layer key
        layer_key = os.path.dirname(layer)

        # pushing the layer in parallel from different images might result in 500 internal server error
        self._logger.debug('Acquiring layer lock', layer_key=layer_key)
        self._layers_lock.get_lock(layer_key).acquire()
        try:

            if layer_key in self._layers_info:
                self._logger.info(
                    'Layer already pushed, skipping',
                    layer_info=self._layers_info[layer_key],
                )
                return self._layers_info['digest'], self._layers_info['size']

            layer_path = os.path.abspath(os.path.join(tmp_dir_name, layer))

            # for Kaniko compatibility - must be real tar.gzip and not just tar
            if os.path.splitext(layer)[1] == '.tar':
                self._logger.debug(
                    'File is not gzipped - compressing before upload',
                    layer_path=layer_path,
                )

                gzip_cmd = shlex.split(f'gzip -9 {layer_path}')
                out = subprocess.check_output(gzip_cmd, encoding='utf-8')
                self._logger.debug('Finished gzip command', gzip_cmd=gzip_cmd, out=out)
                layer += '.gz'
                layer_path += '.gz'

            self._logger.info('Pushing layer', layer=layer)
            push_url = self._initialize_push(image)

            digest, size = self._push_layer(layer_path, push_url)
            self._layers_info[layer_key] = {
                'digest': digest,
                'size': size,
            }
            return digest, size
        finally:
            self._logger.debug('Releasing layer lock', layer_key=layer_key)
            self._layers_lock.get_lock(layer_key).release()

    def _initialize_push(self, repository):
        """
        Request starting an upload for the image repository for a layer or manifest
        """
        self._logger.debug('Initializing push', repository=repository)

        response = requests.post(
            f'{self._registry_url}/v2/{repository}/blobs/uploads/',
            auth=self._basicauth,
            verify=self._ssl_verify,
        )
        upload_url = None
        if response.headers.get("Location", None):
            upload_url = response.headers.get("Location")
        success = response.status_code == 202
        if not success:
            self._logger.log_and_raise(
                'error',
                'Failed to initialize push',
                status_code=response.status_code,
                contents=response.content,
            )
        return upload_url

    def _push_layer(self, layer_path, upload_url):
        return self._chunked_upload(layer_path, upload_url)

    def _push_config(self, config_path, upload_url):
        self._chunked_upload(config_path, upload_url)

    def _chunked_upload(self, filepath, initial_url):
        content_path = os.path.abspath(filepath)
        total_size = os.stat(content_path).st_size

        total_pushed_size = 0
        length_read = 0
        digest = None
        with open(content_path, "rb") as f:
            index = 0
            upload_url = initial_url
            headers = {}
            sha256hash = hashlib.sha256()

            for chunk in self._read_in_chunks(f, sha256hash):
                length_read += len(chunk)
                offset = index + len(chunk)

                if content_path.endswith('gz') or content_path.endswith('gzip'):
                    headers['Content-Encoding'] = 'gzip'

                headers['Content-Type'] = 'application/octet-stream'
                headers['Content-Length'] = str(len(chunk))
                headers['Content-Range'] = f'{index}-{offset}'
                index = offset
                last = False
                if length_read == total_size:
                    last = True
                try:
                    self._conditional_print(
                        "Pushing... "
                        + str(round((offset / total_size) * 100, 2))
                        + "%  ",
                        end="\r",
                    )

                    # complete the upload
                    if last:
                        digest = f'sha256:{str(sha256hash.hexdigest())}'
                        response = requests.put(
                            f"{upload_url}&digest={digest}",
                            data=chunk,
                            headers=headers,
                            auth=self._basicauth,
                            verify=self._ssl_verify,
                        )
                        if response.status_code != 201:
                            self._logger.log_and_raise(
                                'error',
                                'Failed to complete upload',
                                digest=digest,
                                filepath=filepath,
                                status_code=response.status_code,
                                content=response.content,
                            )
                    else:
                        response = requests.patch(
                            upload_url,
                            data=chunk,
                            headers=headers,
                            auth=self._basicauth,
                            verify=self._ssl_verify,
                        )

                        if response.status_code != 202:
                            self._logger.log_and_raise(
                                'error',
                                'Failed to upload chunk',
                                filepath=filepath,
                                status_code=response.status_code,
                                content=response.content,
                            )

                        if "Location" in response.headers:
                            upload_url = response.headers["Location"]

                    total_pushed_size += len(chunk)
                except Exception as exc:
                    self._logger.log_and_raise(
                        'error',
                        'Failed to upload file',
                        filepath=filepath,
                        exc=exc,
                    )

        self._conditional_print("")
        return digest, total_pushed_size

    @staticmethod
    def _read_in_chunks(file_object, hashed, chunk_size=2097152):
        """
        Chunk size default 2T
        """
        while True:
            data = file_object.read(chunk_size)
            hashed.update(data)
            if not data:
                break
            yield data

    @staticmethod
    def _parse_image_tag(image_ref):

        # should be 2 parts exactly
        image, tag = image_ref.split(":")
        return image, tag

    def _replace_tag(self, image, orig_tag):
        if self._replace_tags_match and self._replace_tags_match:
            match_regex = re.compile(self._replace_tags_match)
            if match_regex.match(orig_tag):
                self._logger.info(
                    'Replacing tag for image',
                    image=image,
                    orig_tag=orig_tag,
                    new_tag=self._replace_tags_target,
                )
                return self._replace_tags_target
            else:
                self._logger.debug(
                    'Replace tag match given but did not match',
                    image=image,
                    orig_tag=orig_tag,
                    replace_tags_match=self._replace_tags_match,
                    new_tag=self._replace_tags_target,
                )

        return orig_tag

    @staticmethod
    def _load_json_file(filepath):
        with open(filepath, 'r') as fh:
            return json.loads(fh.read())
