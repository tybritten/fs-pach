#!/usr/bin/env python

from setuptools import find_packages, setup

with open("fs_pach/_version.py") as f:
    exec(f.read())

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Topic :: System :: Filesystems",
]

with open("README.rst", "rt") as f:
    DESCRIPTION = f.read()

REQUIREMENTS = ["pachyderm-sdk>=0.*", "fs~=2.4", "six~=1.10"]

setup(
    name="fs-pachfs",
    author="Tyler Britten",
    author_email="tyler@pachyderm.io",
    classifiers=CLASSIFIERS,
    description="Pachyderm filesystem for PyFilesystem2",
    install_requires=REQUIREMENTS,
    license="MIT",
    long_description=DESCRIPTION,
    packages=find_packages(),
    keywords=["pyfilesystem", "Pachyderm"],
    platforms=["any"],
    test_suite="nose.collector",
    url="https://github.com/tybritten/pachfs",
    version=__version__,
    entry_points={"fs.opener": ["pach = fs_pachfs.opener:PACHFSOpener"]},
)
