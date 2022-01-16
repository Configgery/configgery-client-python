import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="configgery-client",
    version="1.0.2",
    author="Configgery Pty Ltd",
    author_email="support@configgery.com",
    description="Python client for devices interacting with configgery.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Configgery/configgery-client-python",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        "urllib3>=1.21.1"
    ],
)
