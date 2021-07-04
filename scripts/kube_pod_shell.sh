#!/bin/sh

POD=$(kubectl get pod -l app="$1" -o jsonpath="{.items[0].metadata.name}")
kubectl exec --stdin --tty "$POD" -- /bin/sh