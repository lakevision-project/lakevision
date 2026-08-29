#!/bin/bash
# Run Lakevision locally against a real catalog, on one port.
#
# Serves the frontend and proxies /api to the backend from a single origin, the
# way nginx does in the container image -- so relative /api/... fetches work
# without CORS.
#
# Usage:  ./run-local.sh [env-file]     (default: .env.local)
set -euo pipefail

cd "$(dirname "$0")"
ENV_FILE="${1:-.env.local}"
export PORT="${PORT:-8081}"
export BE_PORT="${BE_PORT:-8000}"
export FE_PORT="${FE_PORT:-3000}"

[ -f "$ENV_FILE" ] || { echo "❌ $ENV_FILE not found."; exit 1; }
[ -x be/.venv/bin/python ] || { echo "❌ backend venv missing. Run: make init-be"; exit 1; }

# set -a exports everything the env file defines.
set -a; source "$ENV_FILE"; set +a
# Re-assert ports after sourcing, so the env file cannot silently move them.
export PORT BE_PORT FE_PORT
export PUBLIC_API_SERVER_SERVER_SIDE="http://127.0.0.1:${BE_PORT}"

# The frontend build inlines PUBLIC_* values, so give it its own env file.
mkdir -p fe
: > fe/.env
grep -E '^(PUBLIC_|VITE_)' "$ENV_FILE" >> fe/.env || true
echo "PUBLIC_API_SERVER_SERVER_SIDE=http://127.0.0.1:${BE_PORT}" >> fe/.env

PIDS=()
NAMES=()
cleanup() {
  echo ""
  echo "Shutting down..."
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

LOG_DIR="${TMPDIR:-/tmp}/lakevision-local"
mkdir -p "$LOG_DIR"
echo "  logs: $LOG_DIR"

echo "▶ backend  :${BE_PORT}"
( cd be && PYTHONPATH=app ../be/.venv/bin/python -m uvicorn app.api:app \
    --host 127.0.0.1 --port "${BE_PORT}" --log-level warning ) \
  > "$LOG_DIR/backend.log" 2>&1 &
PIDS+=($!)
NAMES+=("backend")

if [ ! -d fe/build ]; then
  echo "▶ building frontend (first run)"
  ( cd fe && npm run build >/dev/null 2>&1 )
fi

echo "▶ frontend :${FE_PORT}"
( cd fe && PORT="${FE_PORT}" node build/index.js ) \
  > "$LOG_DIR/frontend.log" 2>&1 &
PIDS+=($!)
NAMES+=("frontend")

# Single-origin router: /api -> backend, everything else -> frontend.
echo "▶ proxy    :${PORT}"
node -e '
const http = require("http");
const BE = Number(process.env.BE_PORT), FE = Number(process.env.FE_PORT);
http.createServer((req, res) => {
  const port = req.url.startsWith("/api/") ? BE : FE;
  const p = http.request(
    { host: "127.0.0.1", port, path: req.url, method: req.method,
      headers: { ...req.headers, host: `127.0.0.1:${port}` } },
    (r) => { res.writeHead(r.statusCode, r.headers); r.pipe(res); }
  );
  p.on("error", (e) => { res.writeHead(502); res.end("upstream error: " + e.message); });
  req.pipe(p);
}).listen(Number(process.env.PORT), "127.0.0.1");
' > "$LOG_DIR/proxy.log" 2>&1 &
PIDS+=($!)
NAMES+=("proxy")

# Wait for readiness rather than guessing.
for _ in $(seq 1 120); do
  if curl -sf -m 2 "http://127.0.0.1:${PORT}/api/namespaces" >/dev/null 2>&1; then
    NS=$(curl -s -m 5 "http://127.0.0.1:${PORT}/api/namespaces" | grep -o '"text"' | wc -l | tr -d ' ')
    echo ""
    echo "✅ Lakevision is running:  http://localhost:${PORT}"
    echo "   catalog namespaces: ${NS}"
    echo "   press Ctrl-C to stop"
    echo ""
    # Hold the foreground until a child dies or the user interrupts. `wait`
    # alone returns immediately when this script is not the controlling job.
    while true; do
      for i in "${!PIDS[@]}"; do
        if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
          name="${NAMES[$i]:-service}"
          echo ""
          echo "⚠️  ${name} exited unexpectedly. Last lines of $LOG_DIR/${name}.log:"
          tail -n 15 "$LOG_DIR/${name}.log" 2>/dev/null | sed 's/^/     /'
          echo ""
          echo "   Note: rebuilding the frontend (npm run build) while this is"
          echo "   running replaces fe/build and will kill the frontend process."
          exit 1
        fi
      done
      sleep 2
    done
  fi
  sleep 1
done

echo "❌ did not become ready in time"
exit 1
