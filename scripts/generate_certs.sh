#!/bin/bash
# Script para generar certificados SSL autofirmados para EVCharging
# Uso: ./scripts/generate_certs.sh

set -e

CERTS_DIR="$(dirname "$0")/../certs"
mkdir -p "$CERTS_DIR"

echo "=== Generando certificados para EV_Central (server) ==="
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$CERTS_DIR/server_key.pem" \
    -out "$CERTS_DIR/server_cert.pem" \
    -days 365 \
    -subj "/CN=central/O=EVCharging/C=ES" \
    -addext "subjectAltName=DNS:central,DNS:localhost,IP:127.0.0.1"

echo "=== Generando certificados para EV_Registry ==="
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$CERTS_DIR/registry_key.pem" \
    -out "$CERTS_DIR/registry_cert.pem" \
    -days 365 \
    -subj "/CN=registry/O=EVCharging/C=ES" \
    -addext "subjectAltName=DNS:registry,DNS:localhost,IP:127.0.0.1"

echo ""
echo "=== Certificados generados en $CERTS_DIR ==="
ls -la "$CERTS_DIR"/*.pem
echo ""
echo "Los certificados tienen CN con los nombres DNS de Docker (central, registry)."
echo "Para usar fuera de Docker, regenerar con los hostnames/IPs correctos."
