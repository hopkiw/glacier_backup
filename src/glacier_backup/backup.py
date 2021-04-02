#!/usr/bin/env python3

import logging
import os
import pathlib
import socket
import subprocess
from configparser import ConfigParser
from typing import Generator, TYPE_CHECKING

import boto3

from glacier_backup.glacier import GlacierDB
from glacier_backup.uploader import Uploader

if TYPE_CHECKING:
    from mypy_boto3_glacier.service_resource import Vault
    from mypy_boto3_glacier.service_resource import GlacierServiceResource
else:
    Vault = object
    GlacierServiceResource = object


class AlreadyUploadedException(Exception):
    """This file has already been uploaded"""


class OngoingUploadException(Exception):
    """There is an active upload"""


class Backup(object):
    def __init__(self, config: ConfigParser, dryrun: bool = False):
        self.config = config
        self.dryrun = dryrun
        self._lock()
        self.db = GlacierDB()
        glacier: GlacierServiceResource = boto3.resource("glacier")
        self.vault: Vault = glacier.Vault("-", "default")  # TODO: take as args

        self.uploader = Uploader(self.vault)
        self._setup_logging()

    def _lock(self) -> None:
        try:
            self.s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.s.bind('\0' + self.__class__.__name__)
        except socket.error as e:
            if e.errno == 98:  # 'Address already in use'
                raise OngoingUploadException()
            else:
                raise e

    def _unlock(self) -> None:
        if self.s:
            self.s.close()

    def _setup_logging(self) -> None:
        logging.basicConfig()
        self.log = logging.getLogger()
        self.log.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        home: str = os.environ.get('HOME') or ''
        fh = logging.FileHandler(os.path.join(
            home,
            '.config',
            'glacier_backups',
            'glacier_backups.log'))
        fh.setFormatter(formatter)

        self.log.addHandler(fh)

    def backup_file(self, path: pathlib.Path) -> bool:
        """Backup a single file or directory"""
        self.log.debug(f'backup({path})')

        if self.dryrun:
            self.log.info(f'dry run: would have uploaded {path}')
            return False

        tarpath = ''
        if path.is_dir():
            tarfilename = path.name.replace(' ', '_') + '.tar'
            tarpath = os.path.join('/tmp/glacier_backup', tarfilename)

            self.log.debug(f'tar cf {tarpath} {path}')
            try:
                # Create the tarball.
                subprocess.check_call(['tar', 'cf', tarpath, path.as_posix()])
            except Exception as e:
                self.log.error(f'failed to tar: {e}')
                raise e

        if tarpath:
            archive_id = self.uploader.upload(tarpath, tarfilename)
            if archive_id:
                self.db.mark_uploaded(tarpath, tarfilename, archive_id)
            os.remove(tarpath)
        else:
            archive_id = self.uploader.upload(path.as_posix())
            if archive_id:
                self.db.mark_uploaded(path.as_posix(), path.name, archive_id)

        return True

    def run(self) -> bool:
        """Run all configured backups"""
        sections = self.config.sections()
        for name, cfg in [(x.rstrip('/'), self.config[x]) for x in sections]:
            if not os.path.exists(name):
                self.log.debug(f'skipping nonexistent path {name}')
                continue

            self.log.info(f'starting on {name}')

            upload_if_changed = cfg.getboolean('upload_if_changed')
            upload_single_dir = cfg.getboolean('upload_single_dir')
            upload_dirs = cfg.getboolean('upload_dirs')
            upload_files = cfg.getboolean('upload_files')
            # exclude = cfg.get('exclude_prefix')

            for candidate in self.backup_candidates(
                    name, upload_single_dir,
                    upload_files, upload_dirs, upload_if_changed):
                if self.backup_file(candidate):
                    return True
        return False  # never found an upload or never succeeded.

    def needs_upload(self, file: pathlib.Path,
                     upload_if_changed: bool = False) -> bool:
        uploaded_date = self.db.get_uploaded_date(file.as_posix())
        if not uploaded_date:
            return True
        if upload_if_changed and file.stat().st_mtime > float(uploaded_date):
            return True
        return False

    def backup_candidates(self, path: str,
                          single_dir: bool = False,
                          upload_files: bool = False,
                          upload_dirs: bool = False,
                          upload_if_changed: bool = False
                          ) -> Generator[pathlib.Path, None, None]:
        """Returns a generator of backup candidates."""
        if os.path.isfile(path):
            yield pathlib.Path(path)
            return
        if not os.path.isdir(path):
            return
        if not any([single_dir, upload_files, upload_dirs]):
            return
        if single_dir:
            yield pathlib.Path(path)
            return
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_dir() and upload_dirs:
                    rentry = pathlib.Path(entry)
                    if self.needs_upload(rentry, upload_if_changed):
                        yield rentry
                if entry.is_file() and upload_files:
                    rentry = pathlib.Path(entry)
                    if self.needs_upload(rentry, upload_if_changed):
                        yield rentry


def main():
    import sys
    import argparse
    import configparser

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dryrun',
                        help='only show what would be backed up',
                        action='store_true')
    parser.add_argument('-c', '--config', help='file of backup paths')
    parser.add_argument('-p', '--path', dest='paths', nargs='*',
                        help=('path of file or dir to backup. will override '
                              'paths specified in config'))
    args = parser.parse_args()

    config = configparser.ConfigParser()
    # args.path overrides config.
    if args.paths:
        for path in args.paths:
            # we treat each provided path as the object to be uploaded,
            # whether file or dir.
            config[path] = {'upload_single_dir': True}
    else:
        config.read(args.config or os.path.join(
            os.environ.get('HOME'),
            '.config',
            'glacier_backups',
            'glacier_backups.conf'))

    try:
        Backup(config, args.dryrun).run()
    except OngoingUploadException:
        print('backup already in progress, exiting')
        sys.exit(1)
    except Backup.BackupException as e:
        print('Error: ' + e)
        sys.exit(1)
