#!/bin/bash

set -e

KUBECTL=kubectl
KIND=kind
KIND_CLUSTER_NAME=kwok-cluster
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KWOK_IMAGE="registry.k8s.io/kwok/kwok:v0.7.0"
KWOK_NODE_COUNT=100


$KIND create cluster --name "$KIND_CLUSTER_NAME" --config "$SCRIPT_DIR/kind.yaml"
echo "Waiting for cluster to be ready..."
$KUBECTL cluster-info --context "kind-$KIND_CLUSTER_NAME"
$KUBECTL wait --for=condition=Ready node --all --timeout=5m
echo "Cluster ready"

echo "Loading kwok image..."
if docker image inspect "$KWOK_IMAGE" >/dev/null 2>&1; then
  echo "Image already exists locally"
else
  echo "Pulling image from registry..."
  docker pull "$KWOK_IMAGE"
fi

$KIND load docker-image $KWOK_IMAGE --name $KIND_CLUSTER_NAME
echo "Kwok image loaded"

echo "Deploying kwok..."
$KUBECTL apply -f "$SCRIPT_DIR/kwok.yaml"

echo "Waiting for kwok to be ready..."
$KUBECTL wait --for=condition=Available deploy kwok-controller -n kube-system --timeout=5m
echo "Kwok ready"

echo "Deploying kwok-stage-fast..."
$KUBECTL apply -f "$SCRIPT_DIR/kwok-stage-fast.yaml"

for ((i=0; i<KWOK_NODE_COUNT; i++)); do
  sed "s/#num#/$i/g" "$SCRIPT_DIR/kwok-node.yaml" | kubectl apply -f -
done

helm install keda kedacore/keda --namespace keda --create-namespace
helm install kruise openkruise/kruise --version 1.8.0 --set  manager.image.repository=openkruise-registry.cn-shanghai.cr.aliyuncs.com/openkruise/kruise-manager