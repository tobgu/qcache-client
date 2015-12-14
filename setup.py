# -*- encoding: utf-8 -*-
import glob
import io
import re
import sys
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext
from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['tests', '-s']
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


def read(*names, **kwargs):
    return io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ).read()

__version__ = "0.2.1"

setup(
    name="qcache-client",
    version=__version__,
    license="BSD",
    description="Python client library for QCache",
    long_description="%s\n%s" % (read("README.rst"), re.sub(":obj:`~?(.*?)`", r"``\1``", read("CHANGELOG.rst"))),
    author="Tobias Gustafsson",
    author_email="tobias.l.gustafsson@gmail.com",
    url="https://github.com/tobgu/qcache-client",
    packages=["qclient"],
    py_modules=[splitext(basename(i))[0] for i in glob.glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Topic :: Utilities",
    ],
    keywords=[
        # eg: "keyword1", "keyword2", "keyword3",
    ],
    install_requires=[
        "requests>=2.7.0"
    ],
    extras_require={
        # eg: 'rst': ["docutils>=0.11"],
    },
    tests_require=['pytest', 'qcache'],
    cmdclass={'test': PyTest}

)
