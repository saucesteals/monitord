#!/usr/bin/env bash
#
# Install or update monitord under ~/.monitord.
#
#   infra/install.sh
#   infra/install.sh --ref v1.2.3
#   infra/install.sh --no-restart
#   MONITORD_SERVICE=none infra/install.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECKOUT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

ROOT="${MONITORD_ROOT:-$HOME/.monitord}"
REPO="${MONITORD_REPO:-$(git -C "$CHECKOUT_DIR" remote get-url origin 2>/dev/null || true)}"
REF="${MONITORD_REF:-origin/main}"
BIN_LINK="${MONITORD_BIN_LINK:-$HOME/.local/bin/monitord}"
SERVICE="${MONITORD_SERVICE:-auto}"
SERVICE_NAME="${MONITORD_SERVICE_NAME:-dev.monitord.daemon}"
SERVICE_PATH="${MONITORD_SERVICE_PATH:-$HOME/.local/bin:$HOME/bin:$HOME/go/bin:/opt/homebrew/bin:/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin}"
RESTART=1

usage() {
  sed -n '2,8p' "$0"
  cat <<'EOF'

Options:
  --root PATH       install root (default: $MONITORD_ROOT or ~/.monitord)
  --repo URL        git repository to clone/fetch
  --ref REF         git ref to check out (default: origin/main)
  --service MODE    auto, launchd, systemd, or none
  --no-restart      install files but do not start/restart the service
  --no-service      same as --service none
EOF
}

need_value() {
  if [[ $# -lt 2 || -z "${2:-}" ]]; then
    echo "$1 requires a value" >&2
    exit 2
  fi
}

say() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'monitord install: %s\n' "$*" >&2
  exit 1
}

reject_crlf() {
  local label="$1"
  local value="$2"
  if [[ "$value" == *$'\n'* || "$value" == *$'\r'* ]]; then
    die "$label cannot contain CR or LF characters"
  fi
}

xml_escape() {
  local value="$1"
  value="${value//&/&amp;}"
  value="${value//</&lt;}"
  value="${value//>/&gt;}"
  value="${value//\"/&quot;}"
  value="${value//\'/&apos;}"
  printf '%s' "$value"
}

systemd_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '%s' "$value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root) need_value "$@"; ROOT="$2"; shift 2 ;;
    --repo) need_value "$@"; REPO="$2"; shift 2 ;;
    --ref) need_value "$@"; REF="$2"; shift 2 ;;
    --service) need_value "$@"; SERVICE="$2"; shift 2 ;;
    --no-restart) RESTART=0; shift ;;
    --no-service) SERVICE="none"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$REPO" ]] || die "could not infer repository; pass --repo or set MONITORD_REPO"
[[ "$SERVICE" =~ ^(auto|launchd|systemd|none)$ ]] || die "unsupported service mode: $SERVICE"
[[ "$SERVICE_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9_.@-]*$ ]] || die "MONITORD_SERVICE_NAME must match [A-Za-z0-9][A-Za-z0-9_.@-]*"

reject_crlf "HOME" "$HOME"
reject_crlf "MONITORD_ROOT" "$ROOT"
reject_crlf "MONITORD_REPO" "$REPO"
reject_crlf "MONITORD_REF" "$REF"
reject_crlf "MONITORD_BIN_LINK" "$BIN_LINK"
reject_crlf "MONITORD_SERVICE_NAME" "$SERVICE_NAME"
reject_crlf "MONITORD_SERVICE_PATH" "$SERVICE_PATH"
reject_crlf "XDG_CONFIG_HOME" "${XDG_CONFIG_HOME:-}"

detect_service() {
  if [[ "$SERVICE" != "auto" ]]; then
    printf '%s' "$SERVICE"
    return
  fi

  case "$(uname -s)" in
    Darwin) printf 'launchd' ;;
    Linux)
      if command -v systemctl >/dev/null 2>&1; then
        printf 'systemd'
      else
        printf 'none'
      fi
      ;;
    *) printf 'none' ;;
  esac
}

install_launchd() {
  local plist="$HOME/Library/LaunchAgents/$SERVICE_NAME.plist"
  local domain="gui/$(id -u)"

  mkdir -p "$(dirname "$plist")"
  cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>$(xml_escape "$SERVICE_NAME")</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ProgramArguments</key>
    <array>
      <string>$(xml_escape "$ROOT/run.sh")</string>
    </array>
    <key>StandardOutPath</key>
    <string>$(xml_escape "$ROOT/logs/monitord.log")</string>
    <key>StandardErrorPath</key>
    <string>$(xml_escape "$ROOT/logs/monitord.err.log")</string>
    <key>EnvironmentVariables</key>
    <dict>
      <key>HOME</key>
      <string>$(xml_escape "$HOME")</string>
      <key>MONITORD_ROOT</key>
      <string>$(xml_escape "$ROOT")</string>
      <key>PATH</key>
      <string>$(xml_escape "$SERVICE_PATH")</string>
    </dict>
  </dict>
</plist>
EOF

  say "installed LaunchAgent $plist"
  (( RESTART )) || { say "restart skipped"; return; }

  launchctl bootout "$domain/$SERVICE_NAME" >/dev/null 2>&1 || true
  launchctl unload "$plist" >/dev/null 2>&1 || true
  launchctl enable "$domain/$SERVICE_NAME" >/dev/null 2>&1 || true
  launchctl bootstrap "$domain" "$plist" >/dev/null 2>&1 || launchctl load "$plist"
  launchctl kickstart -k "$domain/$SERVICE_NAME" >/dev/null 2>&1 || true
  say "started $SERVICE_NAME"
}

install_systemd() {
  local unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
  local unit="$unit_dir/$SERVICE_NAME.service"

  mkdir -p "$unit_dir"
  cat > "$unit" <<EOF
[Unit]
Description=monitord background monitor scheduler
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment="HOME=$(systemd_escape "$HOME")"
Environment="MONITORD_ROOT=$(systemd_escape "$ROOT")"
Environment="PATH=$(systemd_escape "$SERVICE_PATH")"
ExecStart=/usr/bin/env bash "$(systemd_escape "$ROOT/run.sh")"
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF

  say "installed systemd user service $unit"
  systemctl --user daemon-reload
  systemctl --user enable "$SERVICE_NAME.service" >/dev/null
  (( RESTART )) || { say "restart skipped"; return; }
  systemctl --user restart "$SERVICE_NAME.service"
  say "started $SERVICE_NAME.service"
}

mkdir -p "$ROOT"/{bin,lib,logs,monitors,artifacts,state}

if [[ -d "$ROOT/lib/.git" ]]; then
  say "fetching $ROOT/lib"
  git -C "$ROOT/lib" remote set-url origin "$REPO"
  git -C "$ROOT/lib" fetch --quiet --tags origin
else
  say "cloning $REPO"
  git clone --quiet "$REPO" "$ROOT/lib"
fi

git -C "$ROOT/lib" checkout --quiet --detach "$REF"
say "source $(git -C "$ROOT/lib" rev-parse --short HEAD)"

say "building monitord"
GOWORK=off go build -C "$ROOT/lib" -o "$ROOT/bin/monitord.next" ./cmd/monitord
mv "$ROOT/bin/monitord.next" "$ROOT/bin/monitord"

mkdir -p "$(dirname "$BIN_LINK")"
ln -sf "$ROOT/bin/monitord" "$BIN_LINK"
"$ROOT/bin/monitord" --root "$ROOT" init >/dev/null

shopt -s nullglob
monitor_dirs=("$ROOT"/monitors/*/)
if (( ${#monitor_dirs[@]} )); then
  say "tidying monitors module"
  (cd "$ROOT/monitors" && GOWORK=off go mod tidy)
fi

install -m 0755 "$ROOT/lib/infra/run.sh" "$ROOT/run.sh"

case "$(detect_service)" in
  launchd) install_launchd ;;
  systemd) install_systemd ;;
  none)
    say "service skipped"
    say "run: $ROOT/bin/monitord --root $ROOT daemon --interval 5s --concurrency 8"
    ;;
esac

say "done"
