#!/usr/bin/env bash
#
# Background-service entrypoint for monitord. The installer copies this to
# $MONITORD_ROOT/run.sh and supplies MONITORD_ROOT through the service manager.
#
set -euo pipefail

export MONITORD_ROOT="${MONITORD_ROOT:-$HOME/.monitord}"

# Service managers start with a small environment. Keep common Go, Homebrew,
# and user-local directories available so future monitor deploys can build.
export PATH="${PATH:-$HOME/.local/bin:$HOME/bin:$HOME/go/bin:/opt/homebrew/bin:/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin}"

# No secrets are sourced here. Routes, proxies, and monitor-owned credentials
# live in monitord's database/state and are delivered to workers explicitly.
exec "$MONITORD_ROOT/bin/monitord" --root "$MONITORD_ROOT" daemon \
  --interval "${MONITORD_INTERVAL:-5s}" \
  --concurrency "${MONITORD_CONCURRENCY:-8}"
