from setuptools import find_packages, setup

setup(
    name='solacc',
    version='0.1',
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=["databricks-sdk==0.32.0", "urllib3==1.26.9"],
    license_files = ('LICENSE',)
)
