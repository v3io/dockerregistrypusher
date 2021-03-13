import tempfile
import subprocess
import multiprocessing.pool
import time
import os.path
import pathlib
import shutil
import json

import humanfriendly

from . import registry
from . import extractor


class Processor(object):
    def __init__(
        self,
        logger,
        tmp_dir,
        tmp_dir_override,
        parallel,
        registry_url,
        archive_path,
        stream=False,
        gzip_layers=False,
        login=None,
        password=None,
        ssl_verify=True,
        replace_tags_match=None,
        replace_tags_target=None,
    ):
        self._logger = logger
        self._parallel = parallel
        self._tmp_dir = tmp_dir
        self._tmp_dir_override = tmp_dir_override
        self._gzip_layers = gzip_layers

        if parallel > 1 and stream:
            self._logger.info(
                'Stream output requested in conjunction with parallel operation. '
                'This will mangle output, disabling stream output'
            )
            stream = False

        self._registry = registry.Registry(
            logger=self._logger,
            registry_url=registry_url,
            stream=stream,
            login=login,
            password=password,
            ssl_verify=ssl_verify,
            replace_tags_match=replace_tags_match,
            replace_tags_target=replace_tags_target,
        )
        self._extractor = extractor.Extractor(self._logger, archive_path)
        self._parallel = parallel

        self._logger.debug('Initialized', parallel=self._parallel)

    def process(self):
        """
        Processing given archive and pushes the images it contains to the registry
        """
        start_time = time.time()
        results = []
        if self._tmp_dir_override:
            tmp_dir_name = self._tmp_dir_override
            os.mkdir(tmp_dir_name, 0o700)
        else:
            tmp_dir_name = tempfile.mkdtemp(dir=self._tmp_dir)

        # since we're not always using TemporaryDirectory we're making and cleaning up ourselves
        try:
            self._logger.info(
                'Processing archive',
                archive_path=self._extractor.archive_path,
                parallel=self._parallel,
                tmp_dir_name=tmp_dir_name,
            )

            # extract the whole thing
            self._extractor.extract_all(tmp_dir_name)

            # pre-process layers in place - for kaniko
            self._pre_process_contents(tmp_dir_name)

            manifest = self._get_manifest(tmp_dir_name)
            self._logger.debug('Extracted archive manifest', manifest=manifest)

            # prepare thread pool, note tarfile is not thread safe https://bugs.python.org/issue23649
            # so if full extraction is not done beforehand, this is not safe
            with multiprocessing.pool.ThreadPool(processes=self._parallel) as pool:
                for image_config in manifest:
                    res = pool.apply_async(
                        process_image,
                        (self._logger, self._registry, tmp_dir_name, image_config),
                    )
                    results.append(res)

                pool.close()
                pool.join()

            # this will throw if any pool worker caught an exception
            for res in results:
                res.get()
        finally:
            shutil.rmtree(tmp_dir_name)
            self._logger.verbose('Removed workdir', tmp_dir_name=tmp_dir_name)

        elapsed = time.time() - start_time
        self._logger.info(
            'Finished processing archive',
            archive_path=self._extractor.archive_path,
            elapsed=humanfriendly.format_timespan(elapsed),
        )

    def _pre_process_contents(self, root_dir):
        start_time = time.time()
        if not self._gzip_layers:
            return

        self._logger.debug('Preprocessing extracted contents')

        # for Kaniko compatibility - must be real tar.gzip and not just tar
        # self._correct_symlinks(root_dir)
        gzipped_layers = self._compress_layers(root_dir)
        self._update_manifests(root_dir, gzipped_layers)

        elapsed = time.time() - start_time
        self._logger.info(
            'Finished preprocessing archive contents',
            elapsed=humanfriendly.format_timespan(elapsed),
        )

    def _compress_layers(self, root_dir):
        """
        we do this in 2 passes, because some layers are symlinked and we re-pointed them to tar.gz earlier, we must
        skip them first, since they are broken symlinks. first gzip all non-linked layers, then do another pass and
        gzip the symlinked ones.
        """
        self._logger.debug(
            'Compressing all layer files (pre-processing)', processes=self._parallel
        )
        gzipped_paths = []
        results = []
        with multiprocessing.pool.ThreadPool(processes=self._parallel) as pool:
            for element in pathlib.Path(root_dir).iterdir():
                if not element.is_dir():
                    continue

                res = pool.apply_async(
                    self._compress_layer,
                    (element,),
                )
                results.append(res)

            pool.close()
            pool.join()

        # this will throw if any pool worker caught an exception
        for res in results:
            gzipped_paths.append(res.get())

        self._logger.debug('Finished compressing all layer files')
        return gzipped_paths

    def _compress_layer(self, layer_dir_path):
        gzipped_layer_path = str(layer_dir_path.absolute()) + '.tar.gz'

        # gzip and keep original (to control the output name)
        tar_cmd = f'tar -czf {gzipped_layer_path} -C {layer_dir_path.parents[0].absolute()} {layer_dir_path.name}'
        self._logger.info('Compressing layer dir', tar_cmd=tar_cmd)
        subprocess.check_call(tar_cmd, shell=True)

        self._logger.debug(
            'Successfully gzipped layer',
            gzipped_layer_path=gzipped_layer_path,
            file_path=layer_dir_path,
        )

        # remove original
        shutil.rmtree(layer_dir_path.absolute())

        return gzipped_layer_path

    def _update_manifests(self, root_dir, gzipped_layers):
        self._logger.debug('Correcting image manifests')
        manifest = self._get_manifest(root_dir)

        for manifest_image_section in manifest:
            for idx, layer in enumerate(manifest_image_section["Layers"]):
                if layer.endswith('.tar'):
                    manifest_image_section["Layers"][idx] = gzipped_layers[idx]

        # write modified image config
        self._write_manifest(root_dir, manifest)
        self._logger.debug('Updated image manifests', manifest=manifest)

    @staticmethod
    def _get_manifest(archive_dir):
        with open(os.path.join(archive_dir, 'manifest.json'), 'r') as fh:
            manifest = json.loads(fh.read())
            return manifest

    @staticmethod
    def _write_manifest(archive_dir, contents):
        with open(os.path.join(archive_dir, 'manifest.json'), 'w') as fh:
            json.dump(contents, fh)


#
# Global wrappers to use with multiprocessing.pool.Pool which can't pickle instance methods
#
def process_image(logger, _registry, tmp_dir_name, image_config):
    try:
        _registry.process_image(tmp_dir_name, image_config)
    except Exception as exc:
        logger.log_and_raise('error', 'Failed processing image', exc=exc)
