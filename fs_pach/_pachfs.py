from __future__ import absolute_import, print_function, unicode_literals

__all__ = ["PACHFS"]

import contextlib
import io
import os
import tempfile
import threading
from ssl import SSLError
from os.path import exists, expanduser
import six
from fs import ResourceType, errors
from fs.base import FS
from fs.info import Info
from fs.mode import Mode
from fs.path import basename, dirname, forcedir, join, normpath, relpath
from fs.subfs import SubFS
from fs.time import datetime_to_epoch
from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs
from pachyderm_sdk.errors import RpcError
from six import text_type


def _make_repr(class_name, *args, **kwargs):
    """
    Generate a repr string.

    Positional arguments should be the positional arguments used to
    construct the class. Keyword arguments should consist of tuples of
    the attribute value and default. If the value is the default, then
    it won't be rendered in the output.

    Here's an example::

        def __repr__(self):
            return make_repr('MyClass', 'foo', name=(self.name, None))

    The output of this would be something line ``MyClass('foo',
    name='Will')``.

    """
    arguments = [repr(arg) for arg in args]
    arguments.extend(
        "{}={!r}".format(name, value)
        for name, (value, default) in sorted(kwargs.items())
        if value != default
    )
    return "{}({})".format(class_name, ", ".join(arguments))


class PachFile(io.IOBase):
    """Proxy for a Pachyderm file. (pfs.File)"""

    @classmethod
    def factory(cls, filename, mode, on_close):
        """Create a PachFile backed with a temporary file."""
        _temp_file = tempfile.TemporaryFile()
        proxy = cls(_temp_file, filename, mode, on_close=on_close)
        return proxy

    def __repr__(self):
        return _make_repr(
            self.__class__.__name__, self.__filename, text_type(self.__mode)
        )

    def __init__(self, f, filename, mode, on_close=None):
        self._f = f
        self.__filename = filename
        self.__mode = mode
        self._on_close = on_close

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def raw(self):
        return self._f

    def close(self):
        if self._on_close is not None:
            self._on_close(self)

    @property
    def closed(self):
        return self._f.closed

    def fileno(self):
        return self._f.fileno()

    def flush(self):
        return self._f.flush()

    def isatty(self):
        return self._f.asatty()

    def readable(self):
        return self.__mode.reading

    def readline(self, limit=-1):
        return self._f.readline(limit)

    def readlines(self, hint=-1):
        if hint == -1:
            return self._f.readlines(hint)
        else:
            size = 0
            lines = []
            for line in iter(self._f.readline, b""):
                lines.append(line)
                size += len(line)
                if size > hint:
                    break
            return lines

    def seek(self, offset, whence=os.SEEK_SET):
        if whence not in (os.SEEK_CUR, os.SEEK_END, os.SEEK_SET):
            raise ValueError("invalid value for 'whence'")
        self._f.seek(offset, whence)
        return self._f.tell()

    def seekable(self):
        return True

    def tell(self):
        return self._f.tell()

    def writable(self):
        return self.__mode.writing

    def writelines(self, lines):
        return self._f.writelines(lines)

    def read(self, n=-1):
        if not self.__mode.reading:
            raise IOError("not open for reading")
        return self._f.read(n)

    def readall(self):
        return self._f.readall()

    def readinto(self, b):
        return self._f.readinto()

    def write(self, b):
        if not self.__mode.writing:
            raise IOError("not open for reading")
        self._f.write(b)
        return len(b)

    def truncate(self, size=None):
        if size is None:
            size = self._f.tell()
        self._f.truncate(size)
        return size


@contextlib.contextmanager
def pacherrors(path):
    """Translate Pachyderm errors to FSErrors."""
    try:
        yield
    except RpcError as error:
        _error = error.response.get("Error", {})
        error_code = _error.get("Code", None)
        response_meta = error.response.get("ResponseMetadata", {})
        http_status = response_meta.get("HTTPStatusCode", 200)
        error_msg = _error.get("Message", None)
        if error_code == "NoSuchBucket":
            raise errors.ResourceError(path, exc=error, msg=error_msg)
        if http_status == 404:
            raise errors.ResourceNotFound(path)
        elif http_status == 403:
            raise errors.PermissionDenied(path=path, msg=error_msg)
        else:
            raise errors.OperationFailed(path=path, exc=error)
    except Exception as error:
        raise errors.RemoteConnectionError(path, exc=error, msg="{}".format(error))


@six.python_2_unicode_compatible
class PACHFS(FS):
    """
    Construct an Pachyderm filesystem for
    `PyFilesystem <https://pyfilesystem.org>`_

    :param str repo_name: The Pachyderm repo name.
    :param str dir_path: The root directory within the Repo.
        Defaults to ``"/"``
    :param str auth_token: The auth token if auth is enabled.
    :param str project_name: The name of the project. Uses default if none supplied.
    :param str host: the dns name or ip of the pachyderm cluster. Uses localhost as default
    :param str port: the port of the pachyderm cluster. Uses 80 as default.
    :param str delimiter: The delimiter to separate folders, defaults to
        a forward slash.
    :param str branch: The branch or commit to use, defaults to master.

    """

    _meta = {
        "case_insensitive": False,
        "invalid_path_chars": "\0",
        "network": True,
        "read_only": False,
        "thread_safe": True,
        "unicode_paths": True,
        "virtual": False,
    }

    _object_attributes = [
        "accept_ranges",
        "cache_control",
        "content_disposition",
        "content_encoding",
        "content_language",
        "content_length",
        "content_type",
        "delete_marker",
        "e_tag",
        "expiration",
        "expires",
        "last_modified",
        "metadata",
        "missing_meta",
        "parts_count",
        "replication_status",
        "request_charged",
        "restore",
        "server_side_encryption",
        "sse_customer_algorithm",
        "sse_customer_key_md5",
        "ssekms_key_id",
        "storage_class",
        "version_id",
        "website_redirect_location",
    ]

    def __init__(
        self,
        repo_name,
        project_name="default",
        dir_path="/",
        auth_token="",
        host="localhost",
        port=80,
        branch="master",
        delimiter="/",
    ):
        self.has_config = False
        if exists(expanduser("~/.pachyderm/config.json")):
            print("FOUND CONFIG")
            self.has_config = True

        self._repo_name = repo_name
        self.dir_path = dir_path
        self._prefix = relpath(normpath(dir_path)).rstrip("/")
        self.auth_token = auth_token
        self.host = host
        self.port = port
        self.branch = branch
        self.project_name = project_name
        self.delimiter = delimiter
        self._tlocal = threading.local()
        super(PACHFS, self).__init__()

    def __repr__(self):
        return _make_repr(
            self.__class__.__name__,
            self._repo_name,
            dir_path=(self.dir_path, "/"),
            delimiter=(self.delimiter, "/"),
        )

    def __str__(self):
        return "<PACHFS '{}'>".format(join(self._repo_name, relpath(self.dir_path)))

    def _path_to_key(self, path):
        """Converts an fs path to a pach repo key."""
        _path = relpath(normpath(path))
        _key = (
            "{}/{}".format(self._prefix, _path).lstrip("/").replace("/", self.delimiter)
        )
        return _key

    def _path_to_dir_key(self, path):
        """Converts an fs path to a pach repo key."""
        _path = relpath(normpath(path))
        _key = (
            forcedir("{}/{}".format(self._prefix, _path))
            .lstrip("/")
            .replace("/", self.delimiter)
        )
        return _key

    def _key_to_path(self, key):
        return key.replace(self.delimiter, "/")

    def _get_object(self, path, key):
        _key = key.rstrip(self.delimiter)
        try:
            with pacherrors(path):
                file_obj = pfs.File.from_uri(
                    f"{self.project_name}/{self._repo_name}@{self.branch}:{_key}"
                )
                obj = self.client.pfs.list_file(file=file_obj)
        except errors.ResourceNotFound:
            with pacherrors(path):
                file_obj = pfs.File.from_uri(
                    f"{self.project_name}/{self._repo_name}@{self.branch}:{_key+self.delimiter}"
                )
                obj = self.client.pfs.list_file(file=file_obj)
                return obj
        else:
            return obj

    @property
    def client(self):
        if not hasattr(self._tlocal, "client"):
            if self.has_config:
                self._tlocal.client = Client.from_config()
            else:
                self._tlocal.client = Client(
                    host=self.host, port=self.port, auth_token=self.auth_token
                )
        return self._tlocal.client

    def _info_from_object(self, pfs_obj: pfs.FileInfo, namespaces=None):
        """Make an info dict from a pfs FileInfo Object."""
        obj = pfs_obj.to_pydict()
        is_dir = True if obj["fileType"] == 2 else False
        info = {"basic": {"name": basename(obj["file"]["path"]), "is_dir": is_dir}}
        _type = int(ResourceType.directory if is_dir else ResourceType.file)
        info["details"] = {
            "modified": datetime_to_epoch(obj["committed"]),
            "size": obj["sizeBytes"] if "sizeBytes" in obj else 0,
            "type": _type,
        }
        return info

    def is_dir(self, path):
        _path = self.validatepath(path)
        try:
            return self.getinfo(_path).is_dir
        except errors.ResourceNotFound:
            return False

    def isfile(self, path):
        _path = self.validatepath(path)
        try:
            return not self.getinfo(_path).is_dir
        except errors.ResourceNotFound:
            return False

    def getinfo(self, path, namespaces=None):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        dir_path = dirname(_path)
        if dir_path != "/":
            with pacherrors(path):
                file_obj = pfs.File.from_uri(
                    f"{self.project_name}/{self._repo_name}@{self.branch}:{_path}"
                )
                obj = self.client.pfs.list_file(file=file_obj)
                if len(list(obj)) == 0:
                    raise errors.ResourceNotFound(path)

        if _path == "/":
            return Info(
                {
                    "basic": {"name": "/", "is_dir": True},
                    "details": {"type": int(ResourceType.directory)},
                }
            )

        obj = self._get_object(_path, _key)
        for file in obj:
            file_obj = file.to_pydict()
            if file_obj["file"]["path"] == _path:
                info = self._info_from_object(file, _key)
                return Info(info)
            else:
                return Info(
                    {
                        "basic": {"name": _key, "is_dir": True},
                        "details": {"type": int(ResourceType.directory)},
                    }
                )

    def _getinfo(self, path, namespaces=None):
        """Gets info without checking for parent dir."""
        namespaces = namespaces or ()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        if _path == "/":
            return Info(
                {
                    "basic": {"name": "", "is_dir": True},
                    "details": {"type": int(ResourceType.directory)},
                }
            )

        obj = self._get_object(path, _key)
        if len(list(obj)) == 0:
            raise errors.ResourceNotFound(path)
        for file in obj:
            if file.to_pydict()["file"]["path"] == path:
                info = self._info_from_object(file)
                return Info(info)
            else:
                return Info(
                    {
                        "basic": {"name": _key, "is_dir": True},
                        "details": {"type": int(ResourceType.directory)},
                    }
                )

    def listdir(self, path):
        _path = self.validatepath(path)
        with pacherrors(path):
            dir_list = self.client.pfs.list_file(
                file=pfs.File.from_uri(
                    f"{self.project_name}/{self._repo_name}@{self.branch}:{_path}"
                )
            )
            _directory = []
            for result in dir_list:
                file_info = result.to_pydict()
                # if file_info["file"]["path"] == f"{_path}/.empty":
                #    continue
                _directory.append(
                    basename(file_info["file"]["path"].rstrip(self.delimiter))
                )

        if not _directory:
            if not self.getinfo(_path).is_dir:
                raise errors.DirectoryExpected(path)
        return _directory

    def makedir(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_dir_key(_path)
        if not self.isdir(dirname(_path)):
            raise errors.ResourceNotFound(path)

        try:
            self._getinfo(path)
        except errors.ResourceNotFound:
            pass
        else:
            raise errors.DirectoryExists(path)
        with pacherrors(path):
            with self.client.pfs.commit(
                branch=pfs.Branch.from_uri(
                    f"{self.project_name}/{self._repo_name}@{self.branch}"
                )
            ) as c:
                keypath = f"{_key}/.empty"
                self.client.pfs.put_file_from_bytes(commit=c, path=keypath, data=b"")
        return SubFS(self, path)

    def openbin(self, path, mode="r", buffering=-1, **options):
        _mode = Mode(mode)
        _mode.validate_bin()
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        if _mode.create:

            def on_close_create(pach_file):
                """Called when the pach file closes, to upload data."""
                try:
                    pach_file.raw.seek(0)
                    with pacherrors(path):
                        with self.client.pfs.commit(
                            branch=pfs.Branch.from_uri(
                                f"{self.project_name}/{self._repo_name}@{self.branch}"
                            )
                        ) as c:
                            self.client.pfs.put_file_from_file(
                                commit=c, path=_key, file=pach_file.raw
                            )
                finally:
                    pach_file.raw.close()

            try:
                dir_path = dirname(_path)
                if dir_path != "/":
                    _dir_key = self._path_to_dir_key(dir_path)
                    self._get_object(dir_path, _dir_key)
            except errors.ResourceNotFound:
                raise errors.ResourceNotFound(path)

            try:
                info = self.getinfo(path)
            except errors.ResourceNotFound:
                pass
            else:
                if _mode.exclusive:
                    raise errors.FileExists(path)
                if info.is_dir:
                    raise errors.FileExpected(path)

            pach_file = PachFile.factory(path, _mode, on_close=on_close_create)
            return pach_file

        def on_close(pach_file):
            """Called when the PACH file closes, to upload the data."""
            try:
                if _mode.writing:
                    pach_file.raw.seek(0, os.SEEK_SET)
                    with pacherrors(path):
                        with self.client.pfs.commit(
                            branch=pfs.Branch.from_uri(
                                f"{self.project_name}/{self._repo_name}@{self.branch}"
                            )
                        ) as c:
                            self.client.pfs.put_file_from_file(
                                commit=c, path=_key, file=pach_file
                            )
            finally:
                pach_file.raw.close()

        pach_file = PachFile.factory(path, _mode, on_close=on_close)
        with pacherrors(path):
            fileobj = pfs.File.from_uri(
                f"{self.project_name}/{self._repo_name}@{self.branch}:{_key}"
            )
            bytestream = self.client.pfs.get_file(file=fileobj)
            for byte in bytestream:
                pach_file.write(byte)
        pach_file.seek(0, os.SEEK_SET)
        return pach_file

    def remove(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        with self.client.pfs.commit(
            branch=pfs.Branch.from_uri(
                f"{self.project_name}/{self._repo_name}@{self.branch}"
            )
        ) as c:
            self.client.pfs.delete_file(commit=c, path=_key)

    def isempty(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_dir_key(_path)
        pfs_file = pfs.File.from_uri(
            f"{self.project_name}/{self._repo_name}@{self.branch}:{_key}"
        )
        contents = self.client.pfs.list_file(file=pfs_file)
        for obj in contents:
            if obj["Key"] != _key and obj["Key"] != ".empty":
                return False
        return True

    def setinfo(self, path, info):
        self.getinfo(path)

    def readbytes(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        bytes_file = io.BytesIO()
        with pacherrors(path):
            fileobj = pfs.File.from_uri(
                f"{self.project_name}/{self._repo_name}@{self.branch}:{_key}"
            )
            bytestream = self.client.pfs.get_file(file=fileobj)
            for byte in bytestream:
                bytes_file.write(byte.value)
        return bytes_file.getvalue()

    def download(self, path, file, **options):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        with pacherrors(path):
            with open(file, "wb") as f:
                fileobj = pfs.File.from_uri(
                    f"{self.project_name}/{self._repo_name}@{self.branch}:{_key}"
                )
                bytestream = self.client.pfs.get_file(file=fileobj)
                for byte in bytestream:
                    f.write(byte.value)

    def exists(self, path):
        self.check()
        _path = self.validatepath(path)
        if _path == "/":
            return True
        _key = self._path_to_dir_key(_path)
        try:
            self._get_object(path, _key)
        except errors.ResourceNotFound:
            return False
        else:
            return True

    def scandir(self, path, namespaces=None, page=None):
        namespaces = namespaces or ()
        info = self.getinfo(path)
        if not info.is_dir:
            raise errors.DirectoryExpected(path)
        with pacherrors(path):
            dir_list = self.client.pfs.list_file(
                file=pfs.File.from_uri(
                    f"{self.project_name}/{self._repo_name}@{self.branch}:{path}"
                )
            )

        def gen_info():
            for obj in dir_list:
                file_obj = self._info_from_object(obj, namespaces)
                yield Info(file_obj)

        iter_info = iter(gen_info())

        for info in iter_info:
            yield info

    def writebytes(self, path, contents):
        if not isinstance(contents, bytes):
            raise TypeError("contents must be bytes")

        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        bytes_file = io.BytesIO(contents)
        with pacherrors(path):
            with self.client.pfs.commit(
                branch=pfs.Branch.from_uri(
                    f"{self.project_name}/{self._repo_name}@{self.branch}"
                )
            ) as c:
                self.client.pfs.put_file_from_bytes(
                    commit=c, path=_key, data=bytes_file
                )

    def upload(self, path, file, chunk_size=None, **options):
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        with pacherrors(path):
            with self.client.pfs.commit(
                branch=pfs.Branch.from_uri(
                    f"{self.project_name}/{self._repo_name}@{self.branch}"
                )
            ) as c:
                self.client.pfs.put_file_from_file(commit=c, path=_key, file=file)

    def copy(self, src_path, dst_path, overwrite=False):
        if not overwrite and self.exists(dst_path):
            raise errors.DestinationExists(dst_path)
        _src_path = self.validatepath(src_path)
        _dst_path = self.validatepath(dst_path)
        _src_key = self._path_to_key(_src_path)
        _dst_key = self._path_to_key(_dst_path)
        try:
            with pacherrors(src_path):
                with self.client.pfs.commit(
                    branch=pfs.Branch.from_uri(
                        f"{self.project_name}/{self._repo_name}@{self.branch}"
                    )
                ) as c:
                    src_file = pfs.File.from_uri(
                        f"{self.project_name}/{self._repo_name}@{self.branch}/{_src_key}"
                    )
                    self.client.pfs.copy_file(commit=c, src=src_file, dst=_dst_key)
        except errors.ResourceNotFound:
            if self.exists(src_path):
                raise errors.FileExpected(src_path)
            raise

    def move(self, src_path, dst_path, overwrite=False):
        self.copy(src_path, dst_path, overwrite=overwrite)
        self.remove(src_path)

    def removedir(self, path):
        self.check()
        _path = self.validatepath(path)
        if _path == "/":
            raise errors.RemoveRootError()
        info = self.getinfo(_path)
        if not info.is_dir:
            raise errors.DirectoryExpected(path)
        if not self.isempty(path):
            raise errors.DirectoryNotEmpty(path)
