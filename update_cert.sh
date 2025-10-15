#!/bin/bash

gskit_path="gsk8capicmd_64"
action="show"
password="changeit"

usage() {
    echo "Usage: $0 --gskit-path <path> [default: ${gskit_path}] \\"
    echo "          --lib-gsk-dir <path> --password <password> [default: ${password}] \\"
    echo "          --action <show|add> [default: ${action}]"
}

validate_ld_path () {

    if [[ ! -x "$1/libgsk8km_64.so" ]]; then
        echo "libgsk8km_64.so not found: $1"
        exit 1
    fi

    export LD_LIBRARY_PATH="$1":$LD_LIBRARY_PATH
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --gskit-path) gskit_path="$2"; shift ;;
        --lib-gsk-dir) validate_ld_path $2; shift ;;
        --password) password="$2"; shift ;;
        --action) action="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; usage; exit 1 ;;
    esac
    shift
done

if ! command -v $gskit_path >/dev/null 2>&1; then
    echo "GSKit executable not found: $gskit_path"
    exit 1
fi

if [[ $action == "show" ]]; then
    $gskit_path -cert -list -db /ssl/keystore.kdb -pw $password

elif [[ $action == "add" ]]; then
    mkdir -p /ssl
    $gskit_path -cert -add -db /ssl/keystore.kdb -pw $password -label "DigiCert G5 TLS RSA4096 SHA384 2021 CA1" -file DigiCertTLSRSA4096RootG5.crt

else
    echo "Invalid action: $action"
    usage
    exit 1
fi