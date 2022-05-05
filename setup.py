from setuptools import setup, find_packages

# Package meta-data.
NAME = "pl_x_cdk_utils"
DESCRIPTION = "Public VNR package with AWS-CDK built in functions and " "boto3 helpers"
URL = "https://github.com/vnrag/pl-x-cdk-utils"
VERSION = "0.1.0"
REQUIRES_PYTHON = ">=3.8.0"

# Packages required
REQUIRED = [
    "boto3",
    "aws-cdk-lib==2.21.1",
    "constructs>=10.0.0,<11.0.0",
    "aws_cdk.aws_glue_alpha",
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
