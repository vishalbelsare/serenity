#!/bin/sh

helm uninstall consul
kubectl delete pvc -l chart=consul-helm
kubectl delete pv -l chart=consul-helm