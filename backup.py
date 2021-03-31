#!/usr/bin/env python3

import os
import sys
import argparse
import configparser

from glacier_backup import Backup, OngoingUploadException

# TODO: review the Backup > Glacier > Uploader classes and division of labor
#       questions we ask: has this file been uploaded already? if so, have we
#       been asked to force? if so, has it been modified since last upload?
#       similarly, are any other uploads ongoing? (why do we check for this? is
#       it just hyper cautiousness bc we were worried about leaking? we have
#       the daemon socket for singleton)
#
#       the logic now is a lot of frame jumps down and further down, both in
#       control structures and in function calls. can we move to a more unit
#       testable model? maybe curate lists of possible results to work on and
#       return, rather than doing work as you list
#
#       what are the domains of work?
#       1. uploading a file to glacier
#          this is already isolated,
#          that could be called 'glacier' and other glacier utils merged
#       2. managing local state. adding entries or confirming presence of
#          entries in the DB
#       3. managing the configuration format and producing possible upload
#          candidates
#
#       what are the nouns of work? backup, glacier, upload, candidate, db,
#       config, path, file, tar
#
#       the user specifies `paths` in a `config`.
#
#       <something> parses the `config` and looks for the files it specifies,
#       which are `candidates`.  this is one layer of selectivity
#
#       <something> checks `candidates` for additional criteria, such as
#       whether they're in `db` and if force is set, whether they've been
#       modified since last upload in `db`
#
#       <something> uploads `candidates`
#
#       <something> marks `candidates` uploaded in the `db`
#
#       supported options:
#       * upload_files
#         uploads files found in the directory
#       * upload_dirs
#         uploads directories found in the directory. NOT recursive.
#       * upload_if_changed
#         the default behavior is to upload each path once. if
#         `upload_if_changed` is set, the behavior will be to upload the path
#         if it has been modified since last upload.
#       * upload_single_dir
#         if path is a directory, upload it instead of any of the files or
#         directories in the pathA
#
#       so again, the 'routing' is impossible to test. but if we first prepare
#       a list, we give up the efficiency of the short-circuit. there may be
#       infinite 'candidate' files to upload, and we may spend a long long
#       time reading each for its candidacy. how can we test and also
#       short-circuit?
#
#       modified time is stored per-file, but options like force are set at
#       path level. so we need both when looking at a file and can't easily
#       split them.


def main():
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
        b = Backup(config, args.dryrun)
        b.run()
    except OngoingUploadException:
        print('backup already in progress, exiting')
        sys.exit(1)
    except Backup.BackupException as e:
        print('Error: ' + e)
        sys.exit(1)


if __name__ == '__main__':
    main()
