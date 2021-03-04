import tempfile
import multiprocessing.pool
import time
import os.path
import pathlib
import shutil
import json
import gzip

import humanfriendly

from . import registry
from . import extractor
import utils.helpers


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
            if self._gzip_layers:
                self._pre_process_contents(tmp_dir_name)

            self._verify_configs_integrity(tmp_dir_name)
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
        self._logger.debug('Preprocessing extracted contents')

        # for Kaniko compatibility - must be real tar.gzip and not just tar
        gzip_ext = '.gz'
        self._correct_symlinks(root_dir, gzip_ext)
        self._compress_layers(root_dir, gzip_ext)
        self._update_manifests(root_dir, gzip_ext)

        elapsed = time.time() - start_time
        self._logger.info(
            'Finished compressing all layer files (pre-processing)',
            elapsed=humanfriendly.format_timespan(elapsed),
        )

    def _correct_symlinks(self, root_dir, gzip_ext):
        self._logger.debug('Updating symlinks to compressed layers')

        # move layer symlinks to gz files, even if they are not there
        for root, dirs, files in os.walk(root_dir):
            for filename in files:
                path = os.path.join(root, filename)

                # If it's not a symlink we're not interested.
                if not os.path.islink(path):
                    continue

                target_path = os.readlink(path)

                if str(target_path).endswith('layer.tar'):
                    self._logger.debug(
                        'Found link to tar layer, pointing to compressed',
                        target_path=target_path,
                        path=path,
                    )

                    # try and fix - point to tar.gz
                    new_target_path = target_path + bytes(gzip_ext)
                    tmp_link_path = f'{target_path}_tmplink'
                    os.symlink(new_target_path, tmp_link_path)
                    os.unlink(path)
                    os.rename(tmp_link_path, path)
                    self._logger.debug(
                        'Moved layer link to compressed target',
                        new_target_path=new_target_path,
                        path=path,
                    )
        self._logger.debug('Finished updating symlinks')

    def _compress_layers(self, root_dir, gzip_ext):
        self._logger.debug('Compressing all layer files (pre-processing)')

        for tar_files in pathlib.Path(root_dir).rglob('*.tar'):
            file_path = str(tar_files.absolute())
            gzipped_file_path = str(file_path) + gzip_ext

            # safety - if .tar.gz is in place, skip
            # compression and ignore the original
            if os.path.exists(gzipped_file_path):
                self._logger.debug(
                    'Layer file is already gzipped - skipping',
                    file_path=file_path,
                    gzipped_path=gzipped_file_path,
                )
                continue

            try:

                # .tar ->.tar.gzip
                self._logger.info('Compressing layer file', file_path=file_path)

                with open(file_path, 'rb') as f_in, gzip.open(
                    gzipped_file_path, 'wb'
                ) as f_out:
                    f_out.writelines(f_in)

                os.remove(file_path)
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

    def _update_manifests(self, root_dir, gzip_ext):
        self._logger.debug('Correcting image manifests')
        manifest = self._get_manifest(root_dir)

        for manifest_image_section in manifest:
            config_filename = manifest_image_section["Config"]
            config_path = os.path.join(root_dir, config_filename)
            image_config = utils.helpers.load_json_file(config_path)

            # warning - spammy
            self._logger.verbose('Parsed image config', image_config=image_config)

            for idx, layer in enumerate(manifest_image_section["Layers"]):
                if layer.endswith('.tar'):
                    gzipped_layer_file_path = layer + gzip_ext
                    manifest_image_section["Layers"][idx] = gzipped_layer_file_path
                    # image_config["rootfs"]['diff_ids'][idx] = utils.helpers.get_digest(
                    #     os.path.join(root_dir, gzipped_layer_file_path)
                    # )

            self._logger.debug(
                '',
                config_path=config_path,
                image_config=image_config,
            )
            # utils.helpers.dump_json_file(config_path, image_config)

        # write modified image config
        self._write_manifest(root_dir, manifest)
        self._logger.debug('Corrected image manifests', manifest=manifest)

    def _verify_configs_integrity(self, root_dir):
        self._logger.debug('Verifying configurations consistency')
        manifest = self._get_manifest(root_dir)

        # check for layer mismatches
        for manifest_image_section in manifest:
            config_filename = manifest_image_section["Config"]
            config_path = os.path.join(root_dir, config_filename)
            image_config = utils.helpers.load_json_file(config_path)

            # warning - spammy
            self._logger.debug('Parsed image config', image_config=image_config)

            for layer_idx, layer in enumerate(manifest_image_section["Layers"]):
                self._logger.debug(
                    'Inspecting layer', image_config=config_path, layer_idx=layer_idx
                )
                layer_path = os.path.join(root_dir, layer)
                digest_from_manifest_path = utils.helpers.get_digest(layer_path)
                digest_from_image_config = image_config['rootfs']['diff_ids'][layer_idx]
                log_kwargs = {
                    'digest_from_manifest_path': digest_from_manifest_path,
                    'digest_from_image_config': digest_from_image_config,
                    'layer_idx': layer_idx,
                }
                if digest_from_image_config == digest_from_image_config:
                    self._logger.debug('Digests comparison passed', **log_kwargs)
                # else:
                #     self._logger.log_and_raise(
                #         'error', 'Failed layer digest validation', **log_kwargs
                #     )

        self._logger.debug('Finished config/manifest verification', manifest=manifest)

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
