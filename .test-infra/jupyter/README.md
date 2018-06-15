This directory contains Jupyter notebooks for use in gathering and analyzing
test metrics.

# Jupyter Setup

Instructions for installing on Linux using pip+virtualenv:

```shell
virtualenv --python python3 ~/virtualenvs/jupyter
source ~/virtualenvs/jupyter/bin/activate
pip install jupyter
# Optional packages, for example:
pip install pandas matplotlib requests
cd .test-infra/jupyter
jupyter notebook  # Should open a browser window.
```

# Pull Requests

To minimize file size, diffs, and ease reviews, please clear all cell output
(cell -> all output -> clear) before commiting.
