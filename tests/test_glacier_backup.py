"""Tests module."""
import configparser
import os
import pathlib

import pytest

from glacier_backup.backup import Backup, OngoingUploadException
from glacier_backup.db import GlacierDB


def test_locking():
    cfg = configparser.ConfigParser()
    # Not in a `try:` block to test this doesn't raise.
    b1 = Backup(cfg)
    b2 = None
    with pytest.raises(Exception) as exc:
        b2 = Backup(cfg)
    b1._unlock()
    assert exc.type == OngoingUploadException
    assert b2 is None


def test_get_candidates_by_path_single(request):
    test_dir = os.path.dirname(request.fspath)
    backup_dir = os.path.join(test_dir, 'test_data', 'backup_dir')

    b = Backup(configparser.ConfigParser())

    candidates = None
    candidates = list(b.backup_candidates_by_path(
        backup_dir,
        single_dir=True,
        upload_files=False,
        upload_dirs=False))
    assert len(candidates) == 1
    assert pathlib.Path(backup_dir) in candidates


def test_get_candidates_by_path_files(request):
    test_dir = os.path.dirname(request.fspath)
    backup_dir = os.path.join(test_dir, 'test_data', 'backup_dir')

    b = Backup(configparser.ConfigParser())

    candidates = None
    candidates = list(b.backup_candidates_by_path(
        backup_dir,
        single_dir=False,
        upload_files=True,
        upload_dirs=False))
    assert len(candidates) == 3
    assert pathlib.Path(os.path.join(backup_dir, 'file1')) in candidates


def test_get_candidates_by_path_dirs(request):
    test_dir = os.path.dirname(request.fspath)
    backup_dir = os.path.join(test_dir, 'test_data', 'backup_dir')

    b = Backup(configparser.ConfigParser())

    candidates = None
    candidates = list(b.backup_candidates_by_path(
        backup_dir,
        single_dir=False,
        upload_files=False,
        upload_dirs=True))
    assert len(candidates) == 3
    assert pathlib.Path(os.path.join(backup_dir, 'dir1')) in candidates


def test_get_candidates_by_path_both(request):
    test_dir = os.path.dirname(request.fspath)
    backup_dir = os.path.join(test_dir, 'test_data', 'backup_dir')

    b = Backup(configparser.ConfigParser())

    candidates = None
    candidates = list(b.backup_candidates_by_path(
        backup_dir,
        single_dir=False,
        upload_files=True,
        upload_dirs=True))
    assert len(candidates) == 6
    assert pathlib.Path(os.path.join(backup_dir, 'file1')) in candidates
    assert pathlib.Path(os.path.join(backup_dir, 'dir1')) in candidates


def test_get_candidates_by_path_single_override(request):
    test_dir = os.path.dirname(request.fspath)
    backup_dir = os.path.join(test_dir, 'test_data', 'backup_dir')

    b = Backup(configparser.ConfigParser())

    candidates = None
    candidates = list(b.backup_candidates_by_path(
        backup_dir,
        single_dir=True,
        upload_files=True,
        upload_dirs=True))
    assert len(candidates) == 1
    assert pathlib.Path(backup_dir) in candidates


def test_get_candidates_by_path_exclude(request):
    test_dir = os.path.dirname(request.fspath)
    backup_dir = os.path.join(test_dir, 'test_data', 'backup_dir')

    b = Backup(configparser.ConfigParser())

    candidates = None
    candidates = list(b.backup_candidates_by_path(
        backup_dir,
        single_dir=False,
        upload_files=True,
        upload_dirs=True,
        exclude='dir1'))
    assert len(candidates) == 5
    assert pathlib.Path(os.path.join(backup_dir, 'dir1')) not in candidates


def test_needs_upload(request):
    test_dir = os.path.dirname(request.fspath)
    cfg = configparser.ConfigParser()
    b = Backup(cfg)
    b._unlock()
    # Override the DB.
    b.db = GlacierDB(os.path.join(test_dir, 'test_data', 'test.sqlite3'))
    assert b.needs_upload(pathlib.Path('/fake/file'))


def test_needs_upload_false(request):
    test_dir = os.path.dirname(request.fspath)
    cfg = configparser.ConfigParser()
    b = Backup(cfg)
    b._unlock()
    # Override the DB.
    b.db = GlacierDB(os.path.join(test_dir, 'test_data', 'test.sqlite3'))
    backup_dir = os.path.join(test_dir, 'test_data', 'backup_dir', 'dir1')
    b.db.mark_uploaded(backup_dir, 'uploaded_as', 'archive_id', 0)
    assert not b.needs_upload(pathlib.Path(backup_dir))


def test_needs_upload_if_changed(request):
    test_dir = os.path.dirname(request.fspath)
    cfg = configparser.ConfigParser()
    b = Backup(cfg)
    b._unlock()
    # Override the DB.
    b.db = GlacierDB(os.path.join(test_dir, 'test_data', 'test.sqlite3'))
    backup_dir = os.path.join(test_dir, 'test_data', 'backup_dir', 'dir1')
    b.db.mark_uploaded(backup_dir, 'uploaded_as', 'archive_id', 1)
    assert b.needs_upload(pathlib.Path(backup_dir), upload_if_changed=True)
