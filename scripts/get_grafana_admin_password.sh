#!/bin/sh

kubectl get secret grafana -o jsonpath="{.data.admin-user}" | base64 --decode ; echo
kubectl get secret grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo