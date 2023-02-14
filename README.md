# PACHFS

PACHFS is a [PyFilesystem](https://www.pyfilesystem.org/) interface to
Pachyderm versioned storage repos.

As a PyFilesystem concrete class, PACHFS allows you to work with Pachyderm repos in the
same way as any other supported filesystem.

## Installing

You can install PACHFS from pip as follows:

```
pip install fs-pach
```

## Opening a PACHFS

Open an PACHFS by explicitly using the constructor:

```python
from fs_pach import PACHFS
pachfs = PACHFS(
      project_name="default",
      repo_name="test",
      host="127.0.0.1",
      port=80,
      auth_token="",
      branch="master",
  )
```

Or with a FS URL:

```python
  from fs import open_fs
  pachfs = open_fs("pach://default/test@master:/")
```

## Downloading Files

To *download* files from an Pachyderm repo, open a file on the PACH
filesystem for reading, then write the data to a file on the local
filesystem. Here's an example that copies a file `example.mov` from
Pachyderm to your HD:

```python
from fs.tools import copy_file_data
with pachfs.open('example.mov', 'rb') as remote_file:
    with open('example.mov', 'wb') as local_file:
        copy_file_data(remote_file, local_file)
```

Although it is preferable to use the higher-level functionality in the
`fs.copy` module. Here's an example:

```python
from fs.copy import copy_file
copy_file(pachfs, 'example.mov', './', 'example.mov')
```

## Uploading Files

You can *upload* files in the same way. Simply copy a file from a
source filesystem to the Pach filesystem.
See [Moving and Copying](https://docs.pyfilesystem.org/en/latest/guide.html#moving-and-copying)
for more information.

## Documentation

- [PyFilesystem Wiki](https://www.pyfilesystem.org)
- [PyFilesystem Reference](https://docs.pyfilesystem.org/en/latest/reference/base.html)
