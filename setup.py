from setuptools import setup

setup(
    name="equicast_pyutils",
    version="0.1",
    packages=["equicast_pyutils"],
    url="https://github.com/coldsofttech/equiCast-pyutils",
    license="MIT",
    author="coldsofttech",
    install_requires=[
        "pandas==2.3.2",
        "pyarrow==21.0.0"
    ],
    requires_python=">=3.10"
)
