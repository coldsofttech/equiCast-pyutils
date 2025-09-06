from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="equicast_pyutils",
    version="0.1.0",
    packages=find_packages(exclude=("tests", "tests.*")),
    long_description=long_description,
    url="https://github.com/coldsofttech/equiCast-pyutils",
    license="MIT",
    author="coldsofttech",
    install_requires=requirements,
    python_requires=">=3.10",
    setup_requires=["setuptools-git-versioning"],
    classifiers=[
        "Programming Language :: Python :: 3.13",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ]
)
