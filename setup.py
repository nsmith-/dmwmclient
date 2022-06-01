#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os.path
from setuptools import (
    setup,
    find_packages,
)


about = {}
with open(os.path.join("dmwmclient", "version.py")) as f:
    exec(f.read(), about)


setup(
    name="dmwmclient",
    version=about["__version__"],
    packages=find_packages(),
    entry_points="""
        [console_scripts]
        dmwm=dmwmclient.cli:cli
    """,
    include_package_data=True,
    description="client and scripts",
    long_description=open("README.md", "rb").read().decode("utf8", "ignore"),
    long_description_content_type="text/markdown",
    maintainer="Nick Smith",
    maintainer_email="nick.smith@cern.ch",
    url="https://github.com/nsmith-/dmwmclient",
    download_url="https://github.com/nsmith-/dmwmclient/releases",
    license="BSD 3-clause",
    test_suite="tests",
    install_requires=["httpx==0.23.0", "lxml", "ipython", "pandas>=1.1.0"],
    extras_require={
        "dev": ["flake8", "black", "pytest-asyncio"],
        "cli": ["matplotlib"],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering :: Physics",
    ],
)
