#!/bin/bash

set -e

KIND=kind
KIND_CLUSTER_NAME=kwok-cluster

$KIND delete cluster --name $KIND_CLUSTER_NAME