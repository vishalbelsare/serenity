#!/usr/bin/env bash

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


if [ ! -x /usr/local/bin/ansible ]; then
    echo "installing Ansible via Homebrew"
    brew install ansible
else
    echo "Ansible is installed"
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
else
    echo "Prometheus is installed"
fi

if [ ! -x /usr/local/bin/grafana-server ]; then
    echo "installing Grafana via Homebrew"
    brew install grafana
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
    pip3 install flake8
else
    echo "Vault is installed"
fi

# set up Helm repositories
helm repo add hashicorp https://helm.releases.hashicorp.com
helm repo add vm https://victoriametrics.github.io/helm-charts/
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

echo "Ready to go!"
