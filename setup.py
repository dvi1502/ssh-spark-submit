import os
from setuptools import setup, find_packages

os.environ['PYTHONDONTWRITEBYTECODE'] = "1"

setup(
    name="ssh-spark-submit",
    version="0.1.0",
    packages=find_packages(),
    author='dvi',
    author_email='dvi1502@mail.ru',
    description='A sample PySpark application',
    zip_safe=False,
    homepage="https://github.com/dvi1502/sftp-spark-submit",
)

