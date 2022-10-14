#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    author="Tenzir",
    author_email="engineering@tenzir.com",
    classifiers=[
        # https://pypi.org/classifiers/
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
        "Topic :: Security",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Software Distribution",
    ],
    description="Python CLI wrapper for VAST - Visibility Across Space and Time",
    include_package_data=True,
    install_requires=[],
    keywords=[
        "vast",
        "pyvast",
        "open source",
        "network telemetry",
    ],
    license="BSD 3-clause",
    long_description=long_description,
    long_description_content_type="text/markdown",
    name="pyvast",
    packages=["pyvast"],
    python_requires=">=3.7",
    setup_requires=["setuptools", "wheel"],
    url="https://github.com/tenzir/vast",
    version="2.3.1",
)
