# coding: utf-8
"""Defines the PACHFS Opener."""

from __future__ import absolute_import, print_function, unicode_literals

__all__ = ["PACHFSOpener"]

from fs.opener import Opener
from fs.opener.errors import OpenerError

from ._pachfs import PACHFS


class PACHFSOpener(Opener):
    protocols = ["pach"]

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        if parse_result.username == "" or parse_result.resource == "":
            raise OpenerError("invalid repo@branch name in '{}'".format(fs_url))
        project, _, repo = parse_result.username.partition("/")
        if _ != "/":
            raise OpenerError("missing project name in '{}'".format(fs_url))
        commit, _, dir_path = parse_result.resource.partition(":")
        if _ != ":":
            dir_path = "/"
        port = int(parse_result.params.get("port", 80))
        pachfs = PACHFS(
            repo_name=repo,
            branch=commit or "master",
            project_name=project or "default",
            dir_path=dir_path,
            auth_token=parse_result.params.get("auth_token", ""),
            host=parse_result.params.get("host", "localhost"),
            port=port,
        )
        return pachfs
