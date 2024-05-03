from setuptools import setup, find_packages

# Package meta-data.
NAME = "pl_x_cdk_utils"
DESCRIPTION = "Public VNR package with AWS-CDK built in functions and " \
              "boto3 helpers"
URL = "https://github.com/vnrag/pl-x-cdk-utils"
VERSION = "0.1.3"
REQUIRES_PYTHON = ">=3.8.0"

# Packages required
REQUIRED = [
        "boto3"
        ]

setup(
        name=NAME,
        version=VERSION,
        description=DESCRIPTION,
        long_description=DESCRIPTION,
        long_description_content_type="text/x-rst",
        python_requires=REQUIRES_PYTHON,
        url=URL,
        license="MIT",
        packages=find_packages(exclude=("test",)),
        install_requires=REQUIRED,
        include_package_data=True,
        classifiers=[
                "Programming Language :: Python :: 3.9",
                "License :: OSI Approved :: MIT License",
                "Operating System :: OS Independent",
                ],
        )
