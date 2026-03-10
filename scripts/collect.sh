#!/usr/bin/env bash
# OpenClaw Usage Collector
# Parses logs → updates data/usage.json → commits to GitHub
# Cron: */30 * * * * /home/mvpbuildsai/openclaw-dashboard/scripts/collect.sh >> /home/mvpbuildsai/openclaw-dashboard/collect.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DATA_FILE="$REPO_DIR/data/usage.json"
LOG_DIR="/tmp/openclaw"
GATEWAY_URL="http://127.0.0.1:18789"
GATEWAY_TOKEN=$(grep -o '"token":"[^"]*"' ~/.openclaw/openclaw.json 2>/dev/null | head -1 | cut -d'"' -f4 || echo "")

# Ensure data file exists
if [ ! -f "$DATA_FILE" ] || [ ! -s "$DATA_FILE" ]; then
  echo '{"sessions":[],"daily":[],"models":{},"agents":{},"lastUpdated":""}' > "$DATA_FILE"
fi

NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
TODAY=$(date -u +"%Y-%m-%d")

# Parse logs for model/session/token events
EVENTS_JSON="[]"
if [ -d "$LOG_DIR" ]; then
  EVENTS_JSON=$(
    find "$LOG_DIR" -name "*.log" 2>/dev/null | \
    xargs grep -h "agent model:\|tokens" 2>/dev/null | \
    python3 -c "
import sys, json, re
events = []
for line in sys.stdin:
    try:
        obj = json.loads(line.strip())
        msg = str(obj.get('1', obj.get('0', '')))
        ts  = obj.get('time', '')
        if 'agent model:' in msg:
            model = msg.split('agent model:')[-1].strip()
            events.append({'type':'model_load','model':model,'ts':ts})
        m = re.search(r'tokens\s+([\d.]+[km]?)/([\d.]+[km]?)\s*\((\d+)%\)', msg, re.I)
        if m:
            events.append({'type':'token_snapshot','used':m.group(1),'limit':m.group(2),'pct':int(m.group(3)),'ts':ts})
    except:
        pass
print(json.dumps(events))
" 2>/dev/null || echo "[]"
  )
fi

# Try gateway API
GATEWAY_STATS="{}"
if [ -n "$GATEWAY_TOKEN" ]; then
  GATEWAY_STATS=$(curl -sf \
    -H "Authorization: Bearer $GATEWAY_TOKEN" \
    "$GATEWAY_URL/api/status" 2>/dev/null || echo "{}")
fi

# Merge into data file
python3 - <<PYEOF
import json
from datetime import datetime

data_file = "$DATA_FILE"
today     = "$TODAY"
now       = "$NOW"

with open(data_file) as f:
    data = json.load(f)

new_events = $EVENTS_JSON
gw_stats   = $GATEWAY_STATS

# Update model usage counts
for ev in new_events:
    if ev.get('type') == 'model_load':
        m = ev['model']
        data['models'][m] = data['models'].get(m, 0) + 1

# Update daily record
daily = {d['date']: d for d in data.get('daily', [])}
if today not in daily:
    daily[today] = {'date': today, 'interactions': 0, 'tokenSnapshots': []}

daily[today]['interactions'] += sum(1 for e in new_events if e.get('type') == 'model_load')
for ev in new_events:
    if ev.get('type') == 'token_snapshot':
        snaps = daily[today]['tokenSnapshots']
        if not snaps or snaps[-1].get('pct') != ev['pct']:
            snaps.append({'pct': ev['pct'], 'ts': ev['ts']})

if isinstance(gw_stats, dict) and gw_stats:
    daily[today]['gatewayStats'] = gw_stats

data['daily'] = sorted(daily.values(), key=lambda x: x['date'])[-90:]
data['lastUpdated'] = now

if isinstance(gw_stats, dict) and 'agents' in gw_stats:
    for a in gw_stats.get('agents', []):
        name = a.get('name', 'unknown')
        data['agents'][name] = data['agents'].get(name, 0) + 1

with open(data_file, 'w') as f:
    json.dump(data, f, indent=2)

print(f"Updated: {len(new_events)} new events, lastUpdated={now}")
PYEOF

# Commit and push
cd "$REPO_DIR"
git add data/usage.json
if git diff --cached --quiet; then
  echo "No changes to commit."
else
  git commit -m "chore: usage snapshot $NOW"
  git push origin main
  echo "Pushed to GitHub."
fi
