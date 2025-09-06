from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="equicast_pyutils",
    version="0.1",
    packages=find_packages(),
    url="https://github.com/coldsofttech/equiCast-pyutils",
    license="MIT",
    author="coldsofttech",
    install_requires=requirements,
    requires_python=">=3.10",
    setup_requires=["setuptools-git-versioning"]
)
