#!/usr/bin/python3

import logging
import os
import sqlite3
import time

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class GlacierDB(object):
    def __init__(self, backupdb: str) -> None:
        """Glacier functions with local state DB"""
        self.conn = sqlite3.connect(backupdb)
        if not os.path.exists(backupdb):
            self.conn.execute('CREATE TABLE IF NOT EXISTS "uploads" (path text, name text, archive_id text,'
                              ' uploaded_date integer')

    def get_uploaded_date(self, filename: str) -> str:
        """Checks if a file has already been uploaded"""
        logger.debug(f'SELECT uploaded_date FROM uploads WHERE path = {filename}')
        res = self.conn.execute('SELECT uploaded_date FROM uploads WHERE path = ?', (filename,))
        all = res.fetchall()
        return all[-1][0] if all else ''

    def mark_uploaded(self, filename: str, uploaded_as: str, archiveid: str, date: int = None) -> None:
        """Marks a file as successfully uploaded"""
        if date is None:
            date = int(time.time())
        with self.conn as cur:
            logger.debug(f'INSERT INTO uploads VALUES ({filename}, {uploaded_as}, {archiveid},{date})')
            cur.execute('INSERT INTO uploads VALUES (?,?,?,?)', (filename, uploaded_as, archiveid, date))
