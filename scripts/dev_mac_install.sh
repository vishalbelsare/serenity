#!/usr/bin/env bash

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
declare xcode_select_installed=$(xcode-select --install 2>&1 | grep "command line tools are already installed")
if [ -z "$xcode_select_installed" ]; then
  echo "Installing xcode-select"
  xcode-select --install
else
  echo "xcode-select installed"
fi

if [ ! -x /usr/local/bin/docker ]; then
  echo "Install Docker for Mac: https://www.docker.com/products/docker#/mac"
  exit 1
else
    echo "Docker is installed"
fi

if [ ! -x /usr/local/bin/brew ]; then
    echo "installing Homebrew"
    /usr/bin/env ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
else
    echo "Homebrew is installed"
fi

if [ ! -x /usr/local/bin/python3.8 ]; then
    echo "installing Python 3.8"
    brew install python@3.8
    brew link --overwrite python@3.8
else
    echo "Python 3.8 is installed"
fi

if [ ! -x /usr/local/bin/timescaledb_move.sh ]; then
    echo "installing TimescaleDB"
    brew tap timescale/tap
    brew install timescaledb

    /usr/local/bin/timescaledb_move.sh
    /usr/local/bin/timescaledb-tune

    brew services restart postgresql
    createuser -s postgres
else
    echo "TimescaleDB is installed"
fi


if [ ! -x /usr/local/bin/minikube ]; then
    echo "installing Minikube (requires sudo)"
    curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-darwin-amd64
    sudo install minikube-darwin-amd64 /usr/local/bin/minikube
    minikube start
else
    echo "Minikube is installed"
fi

if [ ! -x /usr/local/bin/helm ]; then
    echo "installing Helm via Homebrew"
    brew install helm
else
    echo "Helm is installed"
fi

if [ ! -x /usr/local/bin/consul ]; then
    echo "installing Consul via Homebrew"
    brew install consul
else
    echo "Consul is installed"
fi

if [ ! -x /usr/local/bin/prometheus ]; then
    echo "installing Prometheus via Homebrew"
    brew install prometheus
    cp "${script_dir}/../etc/prometheus.yml" /usr/local/etc/prometheus.yml
    brew services start prometheus
else
    echo "Prometheus is installed"
fi

if [ ! -x /usr/local/bin/grafana-server ]; then
    echo "installing Grafana via Homebrew"
    brew install grafana
    brew services start grafana
else
    echo "Grafana is installed"
fi

if [ ! -x /usr/local/bin/vault ]; then
    echo "installing Vault via Homebrew"
    brew install vault
else
    echo "Vault is installed"
fi

if [ ! -x /usr/local/bin/flake8 ]; then
    echo "installing flake8 via pip3"
    /usr/local/bin/pip3 install flake8
else
    echo "flake8 is installed"
fi

if [ ! -e /usr/local/lib/libhdf5.dylib ]; then
    echo "installing HDF5 libraries via Brew"
    brew install hdf5
    brew install c-blosc
else
    echo "HDF5 is installed"
fi

if [ ! -x "${script_dir}/../venv/bin/python3" ]; then
    echo "creating initial virtual environment for Serneity"
    virtualenv venv --python=/usr/local/bin/python3
fi
"${script_dir}/../venv/bin/pip" install --upgrade pip
"${script_dir}/../venv/bin/pip" install -r "$script_dir/../requirements.txt"

# set up Helm repositories
helm repo add hashicorp https://helm.releases.hashicorp.com
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

echo "Ready to go!"
