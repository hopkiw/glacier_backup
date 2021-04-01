#!/usr/bin/env python3

import hashlib
import logging
import math
import os
import queue
import threading

import boto3

# the following helper functions are from boto.glacier.utils. They didn't
# provide equivalents in boto3 that I can find.

_MEGABYTE = 1024 * 1024
DEFAULT_PART_SIZE = 4 * _MEGABYTE
MAXIMUM_NUMBER_OF_PARTS = 10000


def tree_hash(fo):
    """
    Given a hash of each 1MB chunk (from chunk_hashes) this will hash
    together adjacent hashes until it ends up with one big one. So a
    tree of hashes.
    """
    hashes = []
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
    return bytes.hex(hashes[0])


def chunk_hashes(bytestring, chunk_size=_MEGABYTE):
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
    def __init__(self, multipart_upload, work_queue,
                 hash_queue, filename, part_size, err):
        super(UploaderThread, self).__init__()
        self.multipart_upload = multipart_upload
        self.work_queue = work_queue
        self.hash_queue = hash_queue
        self.filename = filename
        self.part_size = part_size
        self.err = err

    def run(self):
        while not self.work_queue.empty() and not self.err.is_set():
            offset = self.work_queue.get()
            logging.info(f'uploading part {offset}')
            try:
                chunk = self.readfile(offset)
                part_hash = self.upload_part(chunk, offset)
            except Exception:
                self.err.set()
                break
            self.hash_queue.put((offset, part_hash))
            self.work_queue.task_done()

    def readfile(self, offset):
        with open(self.filename, 'rb') as fileobj:
            fileobj.seek(offset * self.part_size)
            return fileobj.read(self.part_size)

    def upload_part(self, chunk, offset):
        part_hash = tree_hash(chunk_hashes(chunk))
        hashstr = str(tree_hash(chunk_hashes(chunk)))
        first_byte = offset * self.part_size
        last_byte = first_byte + len(chunk) - 1
        rangestr = f'bytes {first_byte}-{last_byte}/*'
        self.multipart_upload.upload_part(
            range=rangestr,
            checksum=hashstr,
            body=chunk)

        return part_hash


class Uploader():
    class UploadFailedException(Exception):
        pass

    def __init__(self, concurrent_uploads=5, account=None, vault_name=None):
        glacier = boto3.resource('glacier')
        self.vault = glacier.Vault(account or '-', vault_name or 'default')
        self.concurrent_uploads = concurrent_uploads

    def upload(self, filename, description=None):
        description = description or os.path.basename(filename)
        work_queue = queue.Queue()
        hash_queue = queue.Queue()
        filesize = os.stat(filename).st_size
        part_size = DEFAULT_PART_SIZE
        total_parts = int((filesize / part_size) + 1)

        if self.multipart_upload is None:
            self.multipart_upload = self.vault.initiate_multipart_upload(
                archiveDescription=description,
                partSize=str(part_size))

        logging.info(f'created upload {self.multipart_upload.id} for file'
                     f' {filename}. uploading {total_parts} parts')

        for part in range(total_parts):
            work_queue.put(part)

        err = threading.Event()
        ts = []
        for i in range(self.concurrent_uploads):
            t = UploaderThread(
                self.multipart_upload,
                work_queue,
                hash_queue,
                filename,
                part_size,
                err)
            t.daemon = True
            ts.append(t)

        for t in ts:
            t.start()

        for t in ts:
            t.join()

        if err.is_set():
            self.multipart_upload.abort()
            return

        res = [None] * total_parts
        while not hash_queue.empty():
            part, hash_ = hash_queue.get()
            res[part] = hash_
        if None in res:
            logging.error('error uploading parts: missing hash in result')
            return False

        final_checksum = tree_hash(res)
        logging.info(f'completing upload {self.multipart_upload.id}')
        try:
            self.multipart_upload.complete(
                archiveSize=str(filesize),
                checksum=final_checksum)
        except Exception as e:
            logging.error('error completing upload: ' + e)
            self.multipart_upload.abort()
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    args = parser.parse_args()

    uploader = Uploader()
    uploader.upload(args.filename)
