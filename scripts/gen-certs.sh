#!/usr/bin/env bash
# gen-certs.sh — generate a CA and two node certificates for local testing.
# Usage: ./scripts/gen-certs.sh [output_dir]
# Output directory defaults to ./certs
set -euo pipefail

OUTDIR="${1:-certs}"
mkdir -p "$OUTDIR"

DAYS=3650
KEYSIZE=4096

echo "==> Generating CA key and certificate"
openssl genrsa -out "$OUTDIR/ca.key" "$KEYSIZE"
openssl req -new -x509 -days "$DAYS" -key "$OUTDIR/ca.key" \
  -subj "/O=TalariaTest/CN=TalariaCA" \
  -out "$OUTDIR/ca.crt"

gen_node() {
  local name="$1"
  local cn="$2"
  echo "==> Generating $name key and certificate (CN=$cn)"
  openssl genrsa -out "$OUTDIR/$name.key" "$KEYSIZE"
  openssl req -new -key "$OUTDIR/$name.key" \
    -subj "/O=TalariaTest/CN=$cn" \
    -out "$OUTDIR/$name.csr"
  openssl x509 -req -days "$DAYS" \
    -in  "$OUTDIR/$name.csr" \
    -CA  "$OUTDIR/ca.crt" \
    -CAkey "$OUTDIR/ca.key" \
    -CAcreateserial \
    -extfile <(printf "subjectAltName=IP:127.0.0.1,DNS:localhost") \
    -out "$OUTDIR/$name.crt"
  rm "$OUTDIR/$name.csr"
  echo "    $OUTDIR/$name.crt  $OUTDIR/$name.key"
}

gen_node "node1" "talaria-node-1"
gen_node "node2" "talaria-node-2"

echo ""
echo "==> Done.  Files written to $OUTDIR/"
echo "    ca.crt  node1.crt  node1.key  node2.crt  node2.key"
