#!/usr/bin/env bash
# Launches a self-hosted Metis (jupiter-swap-api) instance fed by Yellowstone gRPC.
# The binary is distributed by Jupiter to approved Metis applicants.
set -euo pipefail

RPC_URL="${RPC_URL:?set RPC_URL to a Solana RPC endpoint}"
YELLOWSTONE_GRPC_ENDPOINT="${YELLOWSTONE_GRPC_ENDPOINT:?set YELLOWSTONE_GRPC_ENDPOINT}"
YELLOWSTONE_GRPC_X_TOKEN="${YELLOWSTONE_GRPC_X_TOKEN:-}"

exec ./jupiter-swap-api \
  --rpc-url "$RPC_URL" \
  --yellowstone-grpc-endpoint "$YELLOWSTONE_GRPC_ENDPOINT" \
  ${YELLOWSTONE_GRPC_X_TOKEN:+--yellowstone-grpc-x-token "$YELLOWSTONE_GRPC_X_TOKEN"} \
  --host 127.0.0.1 \
  --port 8080
