#!/usr/bin/env bash
set -euo pipefail

IMAGE_REPO="${IMAGE_REPO:-ghcr.io/firoze-hossain/stratosdb}"
VERSION="${VERSION:-2.0.0}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
PUSH_FLAG="${PUSH_FLAG:---load}"

echo "Building StratosDB ${VERSION} multi-arch container for ${PLATFORMS}..."
docker buildx inspect stratos-multiarch >/dev/null 2>&1 || \
  docker buildx create --name stratos-multiarch --driver docker-container --use

docker buildx build \
  --platform "${PLATFORMS}" \
  -t "${IMAGE_REPO}:${VERSION}" \
  -t "${IMAGE_REPO}:latest" \
  ${PUSH_FLAG} \
  .
