import os
import tarfile
import json
import time

import humanfriendly


class Extractor:
    def __init__(self, logger, archive_path):
        self._logger = logger.get_child('tar')
        self._archive_path = os.path.abspath(archive_path)

        self._logger.debug(
            'Initialized',
            archive_path=self._archive_path,
        )

    @property
    def archive_path(self):
        return self._archive_path

    def get_config(self, name):
        return self._extract_json_from_tar(self._archive_path, name)

    def extract_all(self, target_dir):
        self._logger.info(
            'Extracting', archive_path=self._archive_path, target_dir=target_dir
        )
        start_time = time.time()
        with tarfile.open(self._archive_path) as fh:
            fh.extractall(target_dir)
        elapsed = time.time() - start_time
        self._logger.info(
            'Archive extracted',
            archive_path=self._archive_path,
            target_dir=target_dir,
            elapsed=humanfriendly.format_timespan(elapsed),
        )

    def _extract_json_from_tar(self, tar_filepath, file_to_parse):
        loaded = self._extract_file_from_tar(tar_filepath, file_to_parse)
        stringified = self._parse_as_utf8(loaded)
        return json.loads(stringified)

    @staticmethod
    def _extract_file_from_tar(tar_filepath, file_to_extract):
        tarfile_obj = tarfile.open(tar_filepath)
        file_contents = tarfile_obj.extractfile(file_to_extract)
        return file_contents

    @staticmethod
    def _parse_as_utf8(to_parse):
        as_str = (to_parse.read()).decode("utf-8")
        to_parse.close()
        return as_str
