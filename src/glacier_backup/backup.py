#!/usr/bin/env python3

import logging
import os
import pathlib
import socket
import subprocess
from typing import Generator

import boto3

from glacier_backup.glacier import GlacierDB
from glacier_backup.uploader import Uploader


class AlreadyUploadedException(Exception):
    """This file has already been uploaded"""


class OngoingUploadException(Exception):
    """There is an active upload"""


class Backup(object):
    def __init__(self, config, dryrun=False):
        self.config = config
        self.dryrun = dryrun
        self._lock()
        self.db = GlacierDB()
        glacier = boto3.resource("glacier")
        self.vault = glacier.Vault("-", "default")

        self.uploader = Uploader(self.vault)
        self._setup_logging()

    def _lock(self):
        try:
            self.s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.s.bind('\0' + self.__class__.__name__)
        except socket.error as e:
            if e.errno == 98:  # 'Address already in use'
                raise OngoingUploadException()
            else:
                raise e

    def _unlock(self):
        if self.s:
            self.s.close()

    def _setup_logging(self):
        logging.basicConfig()
        self.log = logging.getLogger()
        self.log.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        fh = logging.FileHandler(os.path.join(
            os.environ.get('HOME'),
            '.config',
            'glacier_backups',
            'glacier_backups.log'))
        fh.setFormatter(formatter)

        self.log.addHandler(fh)

    def backup_file(self, path):
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
                self.log.error('failed to tar: ' + e)
                raise self.BackupException(e)

        try:
            if tarpath:
                archive_id = self.uploader.upload(tarpath, tarfilename)
            else:
                archive_id = self.uploader.upload(path.as_posix())
            self.mark_uploaded(tarpath or path.as_posix(), archive_id)
        except OngoingUploadException as e:
            self.log.error('Failed to upload: ' + e)
            raise e  # cant handle this here

        if tarpath:
            # Remove the tarball.
            os.remove(tarpath)

        return True

    def run(self):
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

    def needs_upload(self, file, upload_if_changed):
        uploaded_date = self.db.get_uploaded_date(file.path)
        # print(f'last uploaded date for {file}: {uploaded_date}')
        if not uploaded_date:
            return True
        if upload_if_changed and file.stat().st_mtime > uploaded_date:
            return True
        return False

    def backup_candidates(self, path, single_dir=False, upload_files=False,
                          upload_dirs=False,
                          upload_if_changed=False) -> Generator[pathlib.Path]:
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
                    if self.needs_upload(entry, upload_if_changed):
                        yield pathlib.Path(entry.path)
                if entry.is_file() and upload_files:
                    if self.needs_upload(entry, upload_if_changed):
                        yield pathlib.Path(entry.path)


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
