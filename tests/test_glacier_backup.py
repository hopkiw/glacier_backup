"""Tests module."""
import os
import pytest
import pathlib

from glacier_backup.backup import Backup, OngoingUploadException


def test_locking():
    # Not in a `try:` block to test this doesn't raise.
    b1 = Backup(None)
    b2 = None
    with pytest.raises(Exception) as exc:
        b2 = Backup(None)
    b1._unlock()
    assert exc.type == OngoingUploadException
    assert b2 is None


def test_get_candidates(request):
    test_dir = os.path.dirname(request.fspath)
    backup_dir = os.path.join(test_dir, 'test_data', 'backup_dir')

    b = Backup(None)

    candidates = None
    candidates = list(b.backup_candidates(
        backup_dir,
        single_dir=True,
        upload_files=False,
        upload_dirs=False))
    assert len(candidates) == 1
    assert pathlib.Path(backup_dir) in candidates

    candidates = None
    candidates = list(b.backup_candidates(
        backup_dir,
        single_dir=False,
        upload_files=True,
        upload_dirs=False))
    assert len(candidates) == 3
    assert pathlib.Path(os.path.join(backup_dir, 'file1')) in candidates

    candidates = None
    candidates = list(b.backup_candidates(
        backup_dir,
        single_dir=False,
        upload_files=False,
        upload_dirs=True))
    assert len(candidates) == 3
    assert pathlib.Path(os.path.join(backup_dir, 'dir1')) in candidates

    candidates = None
    candidates = list(b.backup_candidates(
        backup_dir,
        single_dir=False,
        upload_files=True,
        upload_dirs=True))
    assert len(candidates) == 6
    assert pathlib.Path(os.path.join(backup_dir, 'file1')) in candidates
    assert pathlib.Path(os.path.join(backup_dir, 'dir1')) in candidates

    candidates = None
    candidates = list(b.backup_candidates(
        backup_dir,
        single_dir=True,
        upload_files=True,
        upload_dirs=True))
    assert len(candidates) == 1
    assert pathlib.Path(backup_dir) in candidates

    def test_modified(request):
        # TODO: add if-modified-since tests.
        pass
