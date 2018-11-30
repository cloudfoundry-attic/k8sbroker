#!/bin/bash

set -e

function usage() {
    cat <<EOT
Usage: $(basename $0) <path-to-kube-config> <cf-api-url> <cf-org-name> <cf-space-name>
EOT
}

function main() {
    cd "$(dirname "$0")/.."

    local kubeConfig
    kubeConfig="$1"

    if [[ ! -r "${kubeConfig}" ]]; then
        echo "ERROR: Cannot read Kube config: ${kubeConfig}"
        usage
        exit 1
    fi

    local apiEndpoint
    apiEndpoint="$2"

    if [[ -z "${apiEndpoint}" ]]; then
        echo "ERROR: Please provide the CF API URL to target."
        usage
        exit 1
    fi

    local cfOrg
    cfOrg="$3"

    if [[ -z "${cfOrg}" ]]; then
        echo "ERROR: Please provide the CF org to target."
        usage
        exit 1
    fi

    local cfSpace
    cfSpace="$4"

    if [[ -z "${cfSpace}" ]]; then
        echo "ERROR: Please provide the CF space to target."
        usage
        exit 1
    fi

    if [[ -z "${CF_USERNAME}" ]]; then
        echo "ERROR: Please set CF_USERNAME for cf cli authentication."
        exit 1
    fi

    if [[ -z "${CF_PASSWORD}" ]]; then
        echo "ERROR: Please set CF_PASSWORD for cf cli authentication."
        exit 1
    fi

    cp "${kubeConfig}" kube_config.json

    mkdir -p bin

    echo "Compiling k8sbroker for Linux..."
    GOOS=linux GOARCH=amd64 go build -o bin/k8sbroker

    echo "Targetting CF API..."
    cf api "${apiEndpoint}" --skip-ssl-validation

    echo "Authenticating..."
    cf auth

    echo "Creating org ${cfOrg}..."
    cf create-org "${cfOrg}"

    echo "Creating space ${cfSpace}..."
    cf create-space "${cfSpace}" -o "${cfOrg}"

    echo "Targetting org/space..."
    cf target -o "${cfOrg}" -s "${cfSpace}"

    echo "Pushing k8sbroker app..."
    cf push k8sbroker
}

main "$@"
