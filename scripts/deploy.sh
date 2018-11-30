#!/bin/bash

set -e

function fail() {
    echo "ERROR: $*"
    echo
    usage
    exit 1
}

function usage() {
  cat <<EOT
Usage:
  $(basename $0) <options>

Options:
  -a <app-domain>          - CF app domain
  -i <api-url>             - CF API URL
  -k <path-to-kube-config> - Path to kube-config
  -o <cf-org-name>         - CF org to target
  -s <cf-space-url>        - CF space to target

Environment:
  CF_USERNAME              - CF admin username
  CF_PASSWORD              - CF admin password
EOT
}

function main() {
    local o apiEndpoint appDomain cfOrg cfSpace kubeConfig

    while getopts a:hi:k:o:s: o; do
        case $o in
            a)
                appDomain="${OPTARG}"
                ;;
            h)
                usage
                exit 0
                ;;
            i)
                apiEndpoint="${OPTARG}"
                ;;
            k)
                kubeConfig="${OPTARG}"
                ;;
            o)
                cfOrg="${OPTARG}"
                ;;
            s)
                cfSpace="${OPTARG}"
                ;;
            *)
                echo "Unsupported flag: $o"
                ;;
        esac
    done

    shift $((OPTIND-1))

    cd "$(dirname "$0")/.."

    if [[ -z "${appDomain}" ]]; then
        fail "Please provide the CF app domain."
    fi

    if [[ -z "${apiEndpoint}" ]]; then
        fail "Please provide the CF API URL to target."
    fi

    if [[ -z "${kubeConfig}" ]]; then
        fail "Please provide the path to a valid kube-config file."
    fi

    if [[ ! -r "${kubeConfig}" ]]; then
        fail "Cannot read kube config: ${kubeConfig}"
    fi

    if [[ -z "${cfOrg}" ]]; then
        fail "Please provide the CF org to target."
    fi

    if [[ -z "${cfSpace}" ]]; then
        fail "Please provide the CF space to target."
    fi

    if [[ -z "${CF_USERNAME}" ]]; then
        fail "Please set CF_USERNAME for cf cli authentication."
    fi

    if [[ -z "${CF_PASSWORD}" ]]; then
        fail "Please set CF_PASSWORD for cf cli authentication."
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

    echo "Registering the broker..."
    cf create-service-broker k8sbroker admin admin "https://k8sbroker.${appDomain}"

    echo "Enabling service access..."
    cf enable-service-access nfs
}

main "$@"
