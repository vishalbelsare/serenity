#!/bin/sh

minikube addons enable registry

REGISTRY_POD=$(kubectl get pod -l kubernetes.io/minikube-addons=registry -n kube-system -o jsonpath="{.items[0].metadata.name}")
kubectl port-forward --namespace kube-system "$REGISTRY_POD" 5000:5000 &
docker run --rm -it --network=host alpine ash -c "apk add socat && socat TCP-LISTEN:5000,reuseaddr,fork TCP:host.docker.internal:5000"
