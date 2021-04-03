# Glacier Backup

A utility to manage backups to AWS S3 Glacier.

### Features

Config file specifying paths to be backed up.

Can backup single paths, either files or directories. Directories are uploaded
as tar archives.

Can backup all child directories, all child files, or both.

Can backup files multiple times based on modification date.

No scheduling is provided, but integrates well within cron, systemd or other
batch runners or custom job runners.

### Installation

    pip install glacier-backup

Will install an entrypoint script `glacier-backup`.

### Usage

    usage: glacier-backup [-h] [-d] [-c CONFIG] [-l LOGFILE] [-v VAULT]
                          [-a ACCOUNT] [-p [PATHS [PATHS ...]]]

    optional arguments:
      -h, --help            show this help message and exit
      -d, --dryrun          only show what would be backed up
      -c CONFIG, --config CONFIG
                            config file location. defaults to
                            ${HOME}/.config/glacier_backup/glacier_backup.conf
      -l LOGFILE, --logfile LOGFILE
                            backup log file. defaults to
                            ${HOME}/.config/glacier_backup/glacier_backup.log
      -v VAULT, --vault VAULT
                            name of vault to use
      -a ACCOUNT, --account ACCOUNT
                            account ID to use
      -p [PATHS [PATHS ...]], --path [PATHS [PATHS ...]]
                            path of file or dir to backup. will override paths
                            specified in config

Command line arguments take precedence over configuration options. Paths, vault
name and account ID must all be specified, either in the configuration file or
on the command line.

The `glacier-backup ` utility will scan through the configured paths until it
finds a path matching the configuration criteria (see [behavior](#behavior)
below) and attempt to upload it to S3 Glacier as a single archive. If the path
is a directory, it will be uploaded as a tar archive first.

#### Configuration options

The configuration file follows standard INI syntax, with one (optional) 'main'
section and any number of additional 'path' sections, named after the path that
should be backed up.

The following options are available in the [main] section:

    [main]
    account_id=''
    vault_name=''
    logfile=''

The `account_id` and `vault_name` parameters must be provided either in the
config file or on the command line. The `logfile` parameter is optional, but
does have a default. To disable logging, set `logfile=''` in the configuration
file or `-l ''` on the command line.

The following options are availabile in each [path] section:

    [/path/name]
    upload_single_dir=true
    upload_dirs=true
    upload_files=true
    upload_if_changed=true
    exclude='exclude_'

The section name (`/path/name` above) must be an absolute path and must exist on
the filesystem. 

#### Behavior

`glacier-backup` will resolve possible backup candidates from the paths
specified in the config file, honoring each configuration option as described
below. 

If the path is a file, the path will be considered the only backup candidate.

If the path is a directory and `upload_single_dir` is set to true, the path will
be considered the only backup candidate.

If the path is a directory and `upload_dirs` is set to true, every directory
that is a child of this path will be considered a backup candidate. This
applies to direct children only and is not recursive. This is equivalent to
setting each directory as a path in the configuration file with
`upload_single_dir` set to true.

If the path is a directory and `upload_files`  is set to true, every file that
is a child of this path will be considered a candidate for backup. As with
`upload_dirs`, this is not recursive. This is equivalent to setting each file as
a path in the configuration file.

The `upload_dirs` and `upload_files` options may both be set. If the
`upload_single_dir` option is set, it overrides both of those options. If the
path is a directory and none of these three options are set, no backup
candidates will be found.

Each backup candidate is uploaded by default once, the first time it is
encountered. If `upload_if_changed` is set to true and the file or directory has
been modified since it was uploaded, it will be uploaded again.

If the backup candidate begins with the string set in the `exclude` option,
it will NOT be backed up.

### Scenarios and examples

##### Newly created files

A backup dir with new files appearing in it each day as:

    /path/to/backups/backup_20210320.tar.gz
    /path/to/backups/backup_20210321.tar.gz
    /path/to/backups/backup_20210322.tar.gz
    [...]

Can be backed up with the following section:

    [/path/to/backups]
    upload_files=true


##### Newly created directories

A backup dir with new directories appearing in it each day as:

    /path/to/backups/backup_20210320/
    /path/to/backups/backup_20210321/
    /path/to/backups/backup_20210322/
    [...]

Can be backed up with the following section:

    [/path/to/backups]
    upload_dirs=true

##### Snapshot a directory

A directory with changing content that you want a snapshot of every day can be
backed up with the following section:

    [/path/to/dir]
    upload_single_dir=true
    upload_if_changed=true

This will produce new uploads every time `glacier-backup` runs IF there have
been new changes. Scheduling `glacier-backup` runs should be done with some idea
of the frequency of changes.

The description of the glacier archives will always reflect the filename and
allow duplicates, so the backups should be differentiated by the description and
the uploaded date.

##### Backups are not recursive

The `upload_dirs` and `upload_files` options provide convenience features for
watching the contents of a single directory, but they do not operate
recursively. To illustrate this, consider a directory structure like:

    /path/to/backups/backup_20210320.tar.gz
    /path/to/backups/backup_20210321.tar.gz
    /path/to/backups/backup_20210322.tar.gz
    [...]

Setting a config with `upload_files` as:

    [main]
    vault_name=''
    account_id=''

    [/path/to/backups]
    upload_files=true

Is equivalent to setting each file in the config, as:

    [main]
    vault_name=''
    account_id=''

    [/path/to/backups/backup_20210320.tar.gz]

    [/path/to/backups/backup_20210321.tar.gz]

    [/path/to/backups/backup_20210322.tar.gz]

    [...]

##### Advanced backups

To accomplish backup scenarios that are difficult to manage with the config
syntax, such as recursive backups, consider using the command line to directly
upload paths with the `-p` option in your own scripts. Alternately, you can
generate configuration files with specific paths enumerated.

##### Hacking `glacier-backup`

The entrypoint script for `glacier-backup` calls the `main` function, which
handles configuration, logging, and invoking the backup instance. Use of the
`glacier_backup` library directly should be easy enough for more advanced
scenario, control over logging, etc.
