#!/usr/bin/python3

import os
import sqlite3
import time
import logging
import boto3

from .uploader import Uploader

log = logging.getLogger()


class AlreadyUploadedException(Exception):
    """This file has already been uploaded"""


class OngoingUploadException(Exception):
    """There is an active upload"""


class GlacierDB(object):
    def __init__(self, account='-', vault='default', backupdb=None):
        """Glacier functions with local state DB"""
        self.conn = sqlite3.connect(backupdb or os.path.join(
            os.environ['HOME'],
            '.config',
            'glacier_backups',
            f'glacier.{vault}.sqlite3'))

    def get_uploaded_date(self, filename):
        """Checks if a file has already been uploaded"""
        res = self.conn.execute('SELECT uploaded_date FROM uploads WHERE path '
                                '= ?', (filename,))
        all = res.fetchall()
        return all[-1][0] if all else all

    def mark_uploaded(self, filename, uploaded_as, archiveid):
        """Marks a file as successfully uploaded"""
        date = int(time.time())
        with self.conn as cur:
            log.debug(f'INSERT INTO uploads VALUES ({filename}, {uploaded_as},'
                      f'{archiveid},{date})')
            cur.execute('INSERT INTO uploads VALUES (?,?,?,?)',
                        (filename, uploaded_as, archiveid, date))

