# OpenBB AKShare Extension

## Introduction

This is the OpenBB extension for AKShare.

## Environment Setup
We can run and debug openbb-askshare in a Python virutal environment.
```bash
python3 -m venv .venv
```

Activate the virtual environment:
```bash
source .venv/bin/activate
```
Install the required packages:
```bash
pip install openbb-cli
```

In China, you can use a mirror to speed up the installation process.
```powershell
set PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple
```

## Getting Started

After creating the project, you can test it in an OpenBB environment. From a terminal command line, navigate into the folder where the extension is located, then install the package in "editable" mode.

```bash
cd akshare
pip install -e .
```

After installing `openbb-akshare`, you can verify it using the `pip list` command.

## Rebuild the Python Interface and Static Assets

The application needs to rebuild the static files in order to recognize any changes to the `fetcher_dict` in the `__init__.py` file. This step is also required to reflect changes to parameters, docstrings, and function signatures.

Open a terminal, start a new Python session, and then run the following commands:

```python
cd ../openbb
python
>>> import openbb
>>> openbb.build()
>>> exit()
```
