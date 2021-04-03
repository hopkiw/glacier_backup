#!/usr/bin/env python3

import logging
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
from configparser import ConfigParser
from typing import Generator, cast

import boto3

from botocore.exceptions import ClientError

from glacier_backup.db import GlacierDB
from glacier_backup.uploader import Uploader

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

CONFDIR = os.path.join(cast(str, os.environ.get('HOME')), '.config', 'glacier_backup')


class OngoingUploadException(Exception):
    """There is an active upload"""


class Backup(object):
    def __init__(self, config: ConfigParser, dryrun: bool = False):
        self.config = config
        self.dryrun = dryrun
        self._lock()

        account_id = self.config.get('main', 'account_id', fallback='-')
        vault_name = self.config.get('main', 'vault_name', fallback='default')

        self.db = GlacierDB(os.path.join(CONFDIR, f'glacier.{vault_name}.sqlite3'))

        glacier = boto3.resource('glacier')
        vault = glacier.Vault(account_id, vault_name)
        self.uploader = Uploader(vault)

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

    def backup_file(self, path: pathlib.Path) -> None:
        """Backup a single file or directory"""
        if self.dryrun:
            logger.info(f'dry run: would have uploaded {path}')
            return

        tarpath = ''
        if path.is_dir():
            tarfilename = path.name.replace(' ', '_') + '.tar'
            tempdir = tempfile.mkdtemp(prefix='glacier_backup')
            tarpath = os.path.join(tempdir, tarfilename)

            logger.info(f'creating tar achive for {path}')
            try:
                # Create the tarball.
                subprocess.check_call(['tar', 'cf', tarpath, path.as_posix()], stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL)
            except Exception as e:
                logger.error(f'failed to tar: {e}')
                raise e

        if tarpath:
            upload_file_path = tarpath
            upload_description = tarfilename
        else:
            upload_file_path = path.as_posix()
            upload_description = path.name

        # upload can raise, but we will catch it in run()
        try:
            logger.info(f'starting upload for {path}')
            archive_id = self.uploader.upload(upload_file_path, upload_description)
            self.db.mark_uploaded(path.as_posix(), upload_description, archive_id)
        finally:
            if tarpath:
                os.remove(tarpath)

    def run(self, stop_on_first: bool = True) -> None:
        """Run all configured backups"""
        for candidate in self.backup_candidates():
            try:
                logger.info(f'Starting backup for path {candidate}')
                self.backup_file(candidate)
            except (self.uploader.UploadFailedException, ClientError) as e:
                logger.error(f'failed to upload {candidate}: {e}')
                continue
            if stop_on_first:
                return

    def needs_upload(self, file: pathlib.Path, upload_if_changed: bool = False) -> bool:
        uploaded_date = self.db.get_uploaded_date(file.as_posix())
        if not uploaded_date:
            return True
        if upload_if_changed and file.stat().st_mtime > float(uploaded_date):
            return True
        return False

    def backup_candidates(self) -> Generator[pathlib.Path, None, None]:
        """Run all configured backups"""
        sections = self.config.sections()
        for name, cfg in [(x.rstrip('/'), self.config[x]) for x in sections]:
            if not os.path.exists(name):
                logger.debug(f'skipping nonexistent path {name}')
                continue

            logger.info(f'checking [{name}]')

            upload_if_changed = cfg.getboolean('upload_if_changed')
            upload_single_dir = cfg.getboolean('upload_single_dir')
            upload_dirs = cfg.getboolean('upload_dirs')
            upload_files = cfg.getboolean('upload_files')
            exclude = cfg.get('exclude_prefix')

            yield from self.backup_candidates_by_path(name, upload_single_dir, upload_files, upload_dirs,
                                                      upload_if_changed, exclude)

    def backup_candidates_by_path(self, path: str, single_dir: bool = False, upload_files: bool = False,
                                  upload_dirs: bool = False, upload_if_changed: bool = False,
                                  exclude: str = None) -> Generator[pathlib.Path, None, None]:
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
                if exclude and entry.name.startswith(cast(str, exclude)):
                    continue
                if entry.is_dir() and upload_dirs:
                    rentry = pathlib.Path(entry)
                    if self.needs_upload(rentry, upload_if_changed):
                        yield rentry
                if entry.is_file() and upload_files:
                    rentry = pathlib.Path(entry)
                    if self.needs_upload(rentry, upload_if_changed):
                        yield rentry


def setup_logging(logfile: str = None) -> None:
    # Must use root logger here, not __name__
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if logfile and os.path.exists(logfile):
        fh = logging.FileHandler(logfile, mode='a')
        fh.setFormatter(formatter)
        log.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    log.addHandler(sh)


def main():
    import argparse
    import configparser

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dryrun', help='only show what would be backed up', action='store_true')
    parser.add_argument('-c', '--config', help='config file location. defaults'
                        ' to ${HOME}/.config/glacier_backup/glacier_backup.conf',
                        default=os.path.join(CONFDIR, 'glacier_backup.conf'))
    parser.add_argument('-l', '--logfile', help='backup log file. defaults'
                        ' to ${HOME}/.config/glacier_backup/glacier_backup.log',
                        default=os.path.join(CONFDIR, 'glacier_backup.log'))
    parser.add_argument('-v', '--vault', help='name of vault to use')
    parser.add_argument('-a', '--account', help='account ID to use')
    parser.add_argument('-p', '--path', dest='paths', nargs='*', help=('path of file or dir to backup. will'
                                                                       ' override paths specified in config'))
    args = parser.parse_args()

    config = configparser.ConfigParser()
    # args.path overrides config.
    if args.paths:
        for path in args.paths:
            # we treat each provided path as the object to be uploaded, whether file or dir.
            config[path] = {'upload_single_dir': True}
    else:
        if not os.path.exists(args.config):
            print('no config file found, quitting.')
            return
        config.read(args.config)

    if args.vault:
        config['main']['vault_name'] = args.vault
    if args.account:
        config['main']['account_id'] = args.account

    logfile = config.get('main', 'logfile', fallback=None)
    setup_logging(logfile or args.logfile)

    try:
        Backup(config, args.dryrun).run()
    except OngoingUploadException:
        print('backup already in progress, exiting')
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)
