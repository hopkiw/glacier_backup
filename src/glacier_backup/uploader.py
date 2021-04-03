#!/usr/bin/env python3

import hashlib
import logging
import math
import os
import queue
import threading
from typing import List, TYPE_CHECKING, cast

from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_glacier.service_resource import MultipartUpload
    from mypy_boto3_glacier.service_resource import Vault
else:
    MultipartUpload = object
    Vault = object

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# the following helper functions are from boto.glacier.utils. They didn't
# provide equivalents in boto3 that I can find.

_MEGABYTE = 1024 * 1024
DEFAULT_PART_SIZE = 4 * _MEGABYTE
MAXIMUM_NUMBER_OF_PARTS = 10000


def tree_hash(fo: List[bytes]) -> bytes:
    """
    Given a hash of each 1MB chunk (from chunk_hashes) this will hash
    together adjacent hashes until it ends up with one big one. So a
    tree of hashes.
    """
    hashes: List[bytes] = []
    hashes.extend(fo)
    while len(hashes) > 1:
        new_hashes = []
        while True:
            if len(hashes) > 1:
                first = hashes.pop(0)
                second = hashes.pop(0)
                new_hashes.append(hashlib.sha256(first + second).digest())
            elif len(hashes) == 1:
                only = hashes.pop(0)
                new_hashes.append(only)
            else:
                break
        hashes.extend(new_hashes)
    return hashes[0]


def chunk_hashes(bytestring: bytes, chunk_size: int = _MEGABYTE) -> List[bytes]:
    chunk_count = int(math.ceil(len(bytestring) / float(chunk_size)))
    hashes = []
    for i in range(chunk_count):
        start = i * chunk_size
        end = (i + 1) * chunk_size
        hashes.append(hashlib.sha256(bytestring[start:end]).digest())
    if not hashes:
        return [hashlib.sha256(b'').digest()]
    return hashes


class UploaderThread(threading.Thread):
    def __init__(self, multipart_upload: MultipartUpload, work_queue: queue.Queue, hash_queue: queue.Queue,
                 filename: str, part_size: int, err: threading.Event) -> None:
        super(UploaderThread, self).__init__()
        self.multipart_upload = multipart_upload
        self.work_queue = work_queue
        self.hash_queue = hash_queue
        self.filename = filename
        self.part_size = part_size
        self.err = err

    def run(self) -> None:
        while not self.work_queue.empty() and not self.err.is_set():
            offset = self.work_queue.get()
            logger.debug(f'uploading part {offset}')
            try:
                chunk = self.readfile(offset)
                part_hash = self.upload_part(chunk, offset)
            except Exception:  # Yes, all exceptions.
                self.err.set()
                break
            self.hash_queue.put((offset, part_hash))
            self.work_queue.task_done()

    def readfile(self, offset: int) -> bytes:
        with open(self.filename, 'rb') as fileobj:
            fileobj.seek(offset * self.part_size)
            return fileobj.read(self.part_size)

    def upload_part(self, chunk: bytes, offset: int) -> bytes:
        part_hash = tree_hash(chunk_hashes(chunk))
        hashstr = bytes.hex(part_hash)
        first_byte = offset * self.part_size
        last_byte = first_byte + len(chunk) - 1
        rangestr = f'bytes {first_byte}-{last_byte}/*'
        self.multipart_upload.upload_part(range=rangestr, checksum=hashstr, body=chunk)

        return part_hash


class Uploader():
    class UploadFailedException(Exception):
        pass

    def __init__(self, vault: Vault, concurrent_uploads: int = 5):
        self.vault = vault
        self.concurrent_uploads = concurrent_uploads

    def upload(self, filename: str, description: str = None) -> str:
        description = description or os.path.basename(filename)
        filesize = os.stat(filename).st_size
        part_size = DEFAULT_PART_SIZE

        self.multipart_upload: MultipartUpload
        self.multipart_upload = self.vault.initiate_multipart_upload(archiveDescription=description,
                                                                     partSize=str(part_size))
        logger.debug(f'created multipart_upload {self.multipart_upload.id} for file {filename} with '
                     f'description {description}.')

        try:
            final_checksum = self._upload_threads(filename, filesize, part_size)
        except Exception:
            logger.error(f'aborting {self.multipart_upload.id}')
            self.multipart_upload.abort()
            raise

        logger.info(f'completing upload {self.multipart_upload.id}')
        try:
            ret = self.multipart_upload.complete(archiveSize=str(filesize), checksum=final_checksum)
        except ClientError as e:
            logger.error(f'error completing upload: {e}')
            self.multipart_upload.abort()
            raise

        logger.info(f'upload {self.multipart_upload.id} is complete, archive id is {ret["archiveId"]}')
        return ret['archiveId']

    def _upload_threads(self, filename, filesize, part_size):
        work_queue: queue.Queue = queue.Queue()
        hash_queue: queue.Queue = queue.Queue()

        total_parts = int((filesize / part_size) + 1)
        for part in range(total_parts):
            work_queue.put(part)

        err = threading.Event()
        ts = []
        for i in range(self.concurrent_uploads):
            t = UploaderThread(self.multipart_upload, work_queue, hash_queue, filename, part_size, err)
            t.daemon = True
            ts.append(t)

        for t in ts:
            t.start()
        for t in ts:
            t.join()

        if err.is_set():
            raise self.UploadFailedException('error uploading parts')

        res = [None] * total_parts
        while not hash_queue.empty():
            part, hash_ = hash_queue.get()
            res[part] = hash_
        if None in res:
            raise self.UploadFailedException('error uploading parts: missing hash in result')

        return bytes.hex(tree_hash(cast(List[bytes], res)))


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    args = parser.parse_args()

    uploader = Uploader()
    uploader.upload(args.filename)
