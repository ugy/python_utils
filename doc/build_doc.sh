#!/bin/bash

set -e

./install_dependencies.sh
sphinx-apidoc -e -o ./source ../utils
make html
