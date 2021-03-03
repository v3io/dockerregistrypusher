import tempfile
import multiprocessing.pool
import time
import os.path
import pathlib
import json
import subprocess
import shlex
import gzip

import humanfriendly

from . import registry
from . import extractor
import utils.helpers


class Processor(object):
    def __init__(
        self,
        logger,
        parallel,
        registry_url,
        archive_path,
        stream=False,
        login=None,
        password=None,
        ssl_verify=True,
        replace_tags_match=None,
        replace_tags_target=None,
    ):
        self._logger = logger
        self._parallel = parallel

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
        with tempfile.TemporaryDirectory() as tmp_dir_name:

            self._logger.info(
                'Processing archive',
                archive_path=self._extractor.archive_path,
                parallel=self._parallel,
                tmp_dir_name=tmp_dir_name,
            )

            # extract the whole thing
            self._extractor.extract_all(tmp_dir_name)

            # compress layers in place - for kaniko
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

        elapsed = time.time() - start_time
        self._logger.info(
            'Finished processing archive',
            archive_path=self._extractor.archive_path,
            elapsed=humanfriendly.format_timespan(elapsed),
        )

    def _pre_process_contents(self, root_dir):
        start_time = time.time()
        self._logger.debug('Preprocessing extracted contents')

        # for Kaniko compatibility - must be real tar.gzip and not just tar
        self._compress_layers(root_dir)
        self._correct_symlinks(root_dir)
        self._update_manifests(root_dir)

        elapsed = time.time() - start_time
        self._logger.info(
            'Finished compressing all layer files (pre-processing)',
            elapsed=humanfriendly.format_timespan(elapsed),
        )

    def _compress_layers(self, root_dir):
        self._logger.debug('Compressing all layer files (pre-processing)')
        for tar_files in pathlib.Path(root_dir).rglob('*.tar'):
            file_path = str(tar_files.absolute())
            gzipped_file_path = str(file_path) + '.gz'

            # safety - if .tar.gz is in place, skip
            # compression and ignore the original
            if os.path.exists(gzipped_file_path):
                self._logger.debug(
                    'Layer file is gzipped - skipping',
                    file_path=file_path,
                    gzipped_path=gzipped_file_path,
                )
                continue

            # use -f to avoid "Too many levels of symbolic links" failures
            try:
                # use -f to avoid "Too many levels of symbolic links" failures

                # inplace .tar ->.tar.gz
                self._logger.info('Compressing layer file', file_path=file_path)

                with open(file_path, 'rb') as f_in, gzip.open(gzipped_file_path, 'wb') as f_out:
                    f_out.writelines(f_in)

                self._logger.debug(
                    'Successfully gzipped layer',
                    gzipped_file_path=gzipped_file_path,
                    file_path=file_path,
                )

            except Exception as exc:

                # print debugging info
                layer_dir = pathlib.Path(file_path).parents[0]
                files = layer_dir.glob('**/*')
                self._logger.debug(
                    'Listed elements in layer dir',
                    files=files,
                    layer_dir=layer_dir,
                    exc=exc,
                )
                raise

        self._logger.debug('Finished compressing all layer files')

    def _correct_symlinks(self, root_dir):
        self._logger.debug('Updating symlinks')

        # fix symlinks
        for root, dirs, files in os.walk(root_dir):
            for filename in files:
                path = os.path.join(root, filename)

                # If it's not a symlink we're not interested.
                if not os.path.islink(path):
                    continue

                target_path = os.readlink(path)

                if not os.path.exists(target_path):
                    self._logger.debug(
                        'Found broken link', target_path=target_path, path=path
                    )

                    # try and fix - point to tar.gz
                    new_target_path = target_path + b'.gz'
                    if os.path.exists(new_target_path):
                        tmp_link_path = f'{target_path}_tmplink'
                        os.symlink(new_target_path, tmp_link_path)
                        os.unlink(path)
                        os.rename(tmp_link_path, path)
                        self._logger.debug(
                            'Fixed broken link',
                            new_target_path=new_target_path,
                            path=path,
                        )
                    else:
                        self._logger.log_and_raise(
                            'error',
                            'Cannot fix broken link',
                            target_path=target_path,
                            path=path,
                        )
        self._logger.debug('Finished updating symlinks')

    def _update_manifests(self, root_dir):
        self._logger.debug('Correcting image manifests')
        manifest = self._get_manifest(root_dir)

        for manifest_image_section in manifest:
            config_filename = manifest_image_section["Config"]

            # rootfs.diff_ids contains layer digests - TODO change them?
            config_path = os.path.join(root_dir, config_filename)
            image_config = utils.helpers.load_json_file(config_path)

            # warning - spammy
            self._logger.verbose('Parsed image config', image_config=image_config)

            for idx, layer in enumerate(manifest_image_section["Layers"]):
                if layer.endswith('.tar'):
                    gzipped_layer_file_path = layer + '.gz'
                    manifest_image_section["Layers"][idx] = gzipped_layer_file_path
                    image_config["rootfs"]['diff_ids'][idx] = \
                        utils.helpers.get_digest(os.path.join(root_dir, gzipped_layer_file_path))
            self._logger.debug('Corrected image config', config_path=config_path, image_config=image_config)
            utils.helpers.dump_json_file(config_path, image_config)

        # write modified image config
        self._write_manifest(root_dir, manifest)
        self._logger.debug('Corrected image manifests', manifest=manifest)

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
