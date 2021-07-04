#!/bin/sh

kubectl get secret grafana --output=jsonpath='{.data.admin-password}' | base64 --decode