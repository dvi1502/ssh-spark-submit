import os
from setuptools import setup, find_packages

os.environ['PYTHONDONTWRITEBYTECODE'] = "1"

import os

lib_folder = os.path.dirname(os.path.realpath(__file__))
requirement_path = os.path.join(lib_folder, "requirements.txt")

print(f">>>>>>>>>>> requirement_path = {requirement_path}")

install_requires = []
if os.path.isfile(requirement_path):
    with open(requirement_path, 'r') as file:
         install_requires = [line for line in file.readlines() if line[0] != '#']
         for line in install_requires:
              print(line.strip())

setup(
    name="ssh_spark_submit",
    version="0.1.0",
    packages=find_packages(),
    author='dvi',
    author_email='dvi1502@mail.ru',
    description='A sample PySpark application',
    zip_safe=False,
    install_requires=install_requires,
)
