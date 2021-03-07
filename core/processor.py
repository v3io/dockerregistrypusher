import tempfile
import multiprocessing.pool
import time
import os.path
import shutil
import json

import humanfriendly

from . import registry
from . import extractor


class Processor:
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

        if parallel > 1 and stream:
            self._logger.info(
                'Stream output requested in conjunction with parallel operation. '
                'This will mangle output, disabling stream output'
            )
            stream = False

        self._registry = registry.Registry(
            logger=self._logger,
            gzip_layers=gzip_layers,
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
