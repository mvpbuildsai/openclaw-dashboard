#!/usr/bin/env bash
# OpenClaw Usage Collector — v10
# Cron: */15 * * * * /home/mvpbuildsai/openclaw-dashboard/scripts/collect.sh >> /home/mvpbuildsai/openclaw-dashboard/collect.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
DATA_FILE="$REPO_DIR/data/usage.json"
SNAPSHOT_FILE="$REPO_DIR/data/.last_snapshot.json"
BACKUP_FILE="$HOME/.openclaw/usage_backup.json"

# Find the openclaw CLI (tries PATH first, then common install locations)
OPENCLAW_CMD=$(command -v openclaw 2>/dev/null \
  || { [ -x "$HOME/.npm-global/bin/openclaw" ] && echo "$HOME/.npm-global/bin/openclaw"; } \
  || { [ -x "/home/mvpbuildsai/.npm-global/bin/openclaw" ] && echo "/home/mvpbuildsai/.npm-global/bin/openclaw"; } \
  || echo "")

INIT_JSON='{"sessions":[],"daily":[],"hourly":[],"intervals":[],"models":{},"agents":{},"totals":{"inputTokens":0,"outputTokens":0,"cacheReadTokens":0,"cacheWriteTokens":0,"totalTokens":0,"costUSD":0,"inputCostUSD":0,"outputCostUSD":0,"cacheReadCostUSD":0,"cacheWriteCostUSD":0,"days":0},"lastUpdated":""}'

mkdir -p "$(dirname "$DATA_FILE")" "$(dirname "$BACKUP_FILE")"
[ ! -f "$DATA_FILE" ] || [ ! -s "$DATA_FILE" ] && echo "$INIT_JSON" > "$DATA_FILE"

TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# ———————————————————————————————————————————————————————————————
# BACKUP MERGE
# ———————————————————————————————————————————————————————————————
if [ -f "$BACKUP_FILE" ] && [ -s "$BACKUP_FILE" ]; then
  python3 - "$DATA_FILE" "$BACKUP_FILE" <<'MERGE'
import json, sys
data_file, backup_file = sys.argv[1], sys.argv[2]
try:
    with open(data_file) as f:  cur = json.load(f)
    with open(backup_file) as f: bak = json.load(f)
    cur_days = len(cur.get('daily', []))
    bak_days = len(bak.get('daily', []))
    if bak_days > cur_days:
        merged = {d['date']: d for d in bak.get('daily', [])}
        for d in cur.get('daily', []): merged[d['date']] = d
        cur['daily'] = sorted(merged.values(), key=lambda x: x['date'])
        sess_map = {s['id']: s for s in bak.get('sessions', [])}
        for s in cur.get('sessions', []): sess_map[s['id']] = s
        cur['sessions'] = sorted(sess_map.values(),
            key=lambda s: s.get('updatedAt',''), reverse=True)[:500]
        with open(data_file, 'w') as f: json.dump(cur, f, indent=2)
        print(f"Backup merge: restored {bak_days - cur_days} daily entries")
    else:
        print(f"Backup ok (current={cur_days}, backup={bak_days})")
except Exception as e:
    print(f"Backup merge skipped: {e}")
MERGE
fi

# ———————————————————————————————————————————————————————————————
# COLLECT SESSIONS via openclaw CLI (v9: replaces broken HTTP API)
# ———————————————————————————————————————————————————————————————
NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
TODAY=$(date -u +"%Y-%m-%d")
HOUR_BUCKET=$(date -u +"%Y-%m-%dT%H:00:00Z")

echo "{}" > "$TEMP_DIR/gw_status.json"
echo "[]" > "$TEMP_DIR/gw_sessions.json"

if [ -n "$OPENCLAW_CMD" ]; then
  echo "Sessions: reading via openclaw CLI ($OPENCLAW_CMD)..."

  # CLI returns {"sessions":[...], "count":N, ...} — extract the array
  "$OPENCLAW_CMD" sessions --all-agents --json 2>/dev/null \
    | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    sessions = d.get('sessions', d) if isinstance(d, dict) else d
    json.dump(sessions if isinstance(sessions, list) else [], sys.stdout)
except Exception as e:
    sys.stderr.write(f'  parse error: {e}\n')
    print('[]')
" > "$TEMP_DIR/gw_sessions.json" 2>/dev/null \
    || echo "[]" > "$TEMP_DIR/gw_sessions.json"

  python3 -c "import json; json.load(open('$TEMP_DIR/gw_sessions.json'))" 2>/dev/null \
    || { echo "  WARNING: invalid JSON from CLI, resetting"; echo "[]" > "$TEMP_DIR/gw_sessions.json"; }

  GW_COUNT=$(python3 -c "import json; print(len(json.load(open('$TEMP_DIR/gw_sessions.json'))))" 2>/dev/null || echo "0")
  echo "  Sessions returned: $GW_COUNT"
else
  echo "WARNING: openclaw CLI not found — session data will be empty"
fi

# ———————————————————————————————————————————————————————————————
# MAIN PROCESSING
# ———————————————————————————————————————————————————————————————
python3 - "$DATA_FILE" "$BACKUP_FILE" "$TEMP_DIR" "$NOW" "$TODAY" "$HOUR_BUCKET" "$SNAPSHOT_FILE" <<'PYEOF'
import json, sys, os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

data_file     = sys.argv[1]
backup_file   = sys.argv[2]
temp_dir      = sys.argv[3]
now           = sys.argv[4]
today         = sys.argv[5]
hour_bucket   = sys.argv[6]
snapshot_file = sys.argv[7]

with open(data_file) as f:
    data = json.load(f)

try:
    with open(os.path.join(temp_dir, 'gw_status.json')) as f:
        gw_status = json.load(f)
    if not isinstance(gw_status, dict): gw_status = {}
except Exception:
    gw_status = {}

try:
    with open(os.path.join(temp_dir, 'gw_sessions.json')) as f:
        gw_sessions = json.load(f)
    if not isinstance(gw_sessions, list): gw_sessions = []
except Exception:
    gw_sessions = []

MODEL_COSTS = {
    'gemini-2.5-flash-lite': {'input':0.000075, 'output':0.0003,  'cacheRead':0.0000187, 'cacheWrite':0.000075},
    'gemini-2.5-flash':      {'input':0.00015,  'output':0.0006,  'cacheRead':0.0000375, 'cacheWrite':0.00015},
    'gemini-2.5-pro':        {'input':0.00125,  'output':0.01,    'cacheRead':0.0003125, 'cacheWrite':0.00125},
    'gemini-2.0-flash':      {'input':0.0001,   'output':0.0004,  'cacheRead':0.000025,  'cacheWrite':0.0001},
    'claude-haiku-4-5':      {'input':0.0008,   'output':0.004,   'cacheRead':0.00008,   'cacheWrite':0.001},
    'claude-sonnet-4-6':     {'input':0.003,    'output':0.015,   'cacheRead':0.0003,    'cacheWrite':0.00375},
    'claude-opus-4-6':       {'input':0.015,    'output':0.075,   'cacheRead':0.0015,    'cacheWrite':0.01875},
}
DEFAULT_COST = {'input':0.001,'output':0.005,'cacheRead':0.0001,'cacheWrite':0.0005}

def get_rates(model):
    if model in MODEL_COSTS: return MODEL_COSTS[model]
    for k,v in MODEL_COSTS.items():
        if model.startswith(k): return v
    return DEFAULT_COST

def safe_int(v):
    try: return int(v or 0)
    except: return 0
def safe_float(v):
    try: return float(v or 0.0)
    except: return 0.0
def to_hour(ts):
    return ts[:13]+':00:00Z' if ts and len(ts)>=13 else ''
def to_date(ts):
    return ts[:10] if ts and len(ts)>=10 else ''

def process_session(raw):
    # Handle updatedAt as ms integer (CLI format) or ISO string (legacy gateway)
    raw_ts = raw.get('updatedAt') or raw.get('updated_at') or raw.get('lastActivity') or ''
    if isinstance(raw_ts, (int, float)) and raw_ts > 1e10:
        updated = datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    elif raw_ts:
        updated = str(raw_ts)
    else:
        updated = now

    # Derive channel from CLI session key ("agent:AGENT:CHANNEL:kind:id")
    key = raw.get('key', '')
    channel_raw = raw.get('channel') or raw.get('channelType') or raw.get('source') or ''
    if not channel_raw and key:
        parts = key.split(':')
        channel_raw = parts[2] if len(parts) >= 3 else 'unknown'
    channel = (channel_raw or 'unknown').strip() or 'unknown'

    inp  = safe_int(raw.get('inputTokens')  or raw.get('input_tokens'))
    out  = safe_int(raw.get('outputTokens') or raw.get('output_tokens'))
    ctx  = safe_int(raw.get('contextTokens') or raw.get('context_tokens'))
    model   = (raw.get('model') or raw.get('modelName') or 'unknown').strip() or 'unknown'
    # agentId is the CLI field; agent/agentName/agent_id are legacy
    agent = (raw.get('agent') or raw.get('agentName') or raw.get('agentId')
             or raw.get('agent_id') or 'unknown').strip() or 'unknown'
    prov  = raw.get('modelProvider') or raw.get('provider') or ''

    # totalTokens: use input+output when available (CLI totalTokens is context window, not sum)
    gw_tot = safe_int(raw.get('totalTokens') or raw.get('total_tokens'))
    total  = inp + out if (inp + out) > 0 else gw_tot

    cr = safe_int(raw.get('cacheReadTokens') or raw.get('cache_read_tokens') or
                  raw.get('cachedInputTokens') or raw.get('cached_input_tokens'))
    cw = safe_int(raw.get('cacheWriteTokens') or raw.get('cache_write_tokens') or
                  raw.get('cacheCreationInputTokens') or raw.get('cache_creation_input_tokens'))

    # Normalize model name (strip provider prefix)
    for prefix in ('google/', 'anthropic/', 'openai/', 'meta/', 'mistral/'):
        if model.startswith(prefix):
            model = model[len(prefix):]
            break

    r = get_rates(model)
    inp_c = round(inp * r['input']     / 1000, 8)
    out_c = round(out * r['output']    / 1000, 8)
    cr_c  = round(cr  * r['cacheRead'] / 1000, 8)
    cw_c  = round(cw  * r['cacheWrite']/ 1000, 8)
    gw_cost = safe_float(raw.get('estimatedCostUSD') or raw.get('cost_usd'))
    cost = gw_cost if gw_cost > 0 else round(inp_c+out_c+cr_c+cw_c, 8)

    return {
        'id': (raw.get('id') or raw.get('sessionId') or '').strip(),
        'agent': agent, 'channel': channel, 'model': model,
        'modelProvider': prov, 'contextTokens': ctx,
        'inputTokens': inp, 'outputTokens': out,
        'cacheReadTokens': cr, 'cacheWriteTokens': cw,
        'totalTokens': total, 'estimatedCostUSD': cost,
        'updatedAt': updated, 'date': to_date(updated),
        '_ic': inp_c, '_oc': out_c, '_rc': cr_c, '_wc': cw_c,
    }

# —— 1. MERGE SESSIONS ———————————————————————————————————————————
sess_map = {}
for s in data.get('sessions', []):
    sid = (s.get('id') or '').strip()
    if sid:
        h = process_session(s)
        sess_map[h['id']] = h

for gs in gw_sessions:
    sid = (gs.get('id') or gs.get('sessionId') or gs.get('session_id') or '').strip()
    if sid:
        p = process_session(gs)
        if p['id']: sess_map[p['id']] = p

STRIP = {'_ic','_oc','_rc','_wc'}
all_sessions = sorted(sess_map.values(), key=lambda s: s.get('updatedAt',''), reverse=True)[:500]
data['sessions'] = [{k:v for k,v in s.items() if k not in STRIP} for s in all_sessions]

# —— 1b. DELTA BACKFILL — fill historical gaps using snapshot baseline ——
#
# If a session was last active on a past date that has a zero daily entry,
# compute the delta from the last known snapshot and attribute it to that date.
#
snapshot_baseline = {}
try:
    if os.path.exists(snapshot_file):
        with open(snapshot_file) as f:
            snap = json.load(f)
        raw_snaps = snap.get('sessions', {})
        if isinstance(raw_snaps, dict):
            # v1 format: {"sessions": {"uuid": {inputTokens, outputTokens, ...}}}
            for uuid, sv in raw_snaps.items():
                snapshot_baseline[uuid] = {
                    'inputTokens':      safe_int(sv.get('inputTokens', 0)),
                    'outputTokens':     safe_int(sv.get('outputTokens', 0)),
                    'totalTokens':      safe_int(sv.get('totalTokens', 0)),
                    'cacheReadTokens':  safe_int(sv.get('cacheReadTokens', 0)),
                    'cacheWriteTokens': safe_int(sv.get('cacheWriteTokens', 0)),
                }
        elif isinstance(raw_snaps, list):
            # v2 format: list of session objects (same as all_sessions)
            for sv in raw_snaps:
                uid = (sv.get('id') or sv.get('sessionId') or '').strip()
                if uid:
                    snapshot_baseline[uid] = {
                        'inputTokens':      safe_int(sv.get('inputTokens', 0)),
                        'outputTokens':     safe_int(sv.get('outputTokens', 0)),
                        'totalTokens':      safe_int(sv.get('totalTokens', 0)),
                        'cacheReadTokens':  safe_int(sv.get('cacheReadTokens', 0)),
                        'cacheWriteTokens': safe_int(sv.get('cacheWriteTokens', 0)),
                    }
    print(f"  Snapshot baseline: {len(snapshot_baseline)} sessions")
except Exception as e:
    print(f"  Snapshot load failed: {e}")

# Build the daily_map early so we can update historical entries
daily_map_pre = {d['date']: d for d in data.get('daily', [])}
backfill_count = 0
cutoff_date = (datetime.now(tz=timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')

for s in all_sessions:
    s_date = s.get('date') or to_date(s.get('updatedAt', ''))
    if not s_date or s_date == today or s_date < cutoff_date:
        continue
    existing = daily_map_pre.get(s_date)
    # Only backfill days with no token data
    if existing and (safe_int(existing.get('totalTokens', 0)) > 0 or
                     safe_int(existing.get('sessions', 0)) > 0):
        continue
    # Compute delta against snapshot baseline
    sid = s.get('id', '')
    if not sid:
        continue
    base = snapshot_baseline.get(sid, {})
    delta_inp = max(0, s['inputTokens']      - safe_int(base.get('inputTokens', 0)))
    delta_out = max(0, s['outputTokens']     - safe_int(base.get('outputTokens', 0)))
    delta_cr  = max(0, s.get('cacheReadTokens', 0)  - safe_int(base.get('cacheReadTokens', 0)))
    delta_cw  = max(0, s.get('cacheWriteTokens', 0) - safe_int(base.get('cacheWriteTokens', 0)))
    delta_tot = delta_inp + delta_out

    if delta_tot == 0 and delta_inp == 0:
        continue

    if s_date not in daily_map_pre:
        daily_map_pre[s_date] = {
            'date': s_date, 'inputTokens': 0, 'outputTokens': 0,
            'cacheReadTokens': 0, 'cacheWriteTokens': 0, 'totalTokens': 0,
            'costUSD': 0.0, 'inputCostUSD': 0.0, 'outputCostUSD': 0.0,
            'cacheReadCostUSD': 0.0, 'cacheWriteCostUSD': 0.0,
            'sessions': 0, 'models': {}, 'agents': {}, 'channels': {},
            '_contributions': {},
        }

    past_entry = daily_map_pre[s_date]
    past_contrib = past_entry.get('_contributions', {})
    old = past_contrib.get(sid, {})
    r = get_rates(s.get('model', 'unknown'))
    past_contrib[sid] = {
        'inputTokens':      max(delta_inp, safe_int(old.get('inputTokens', 0))),
        'outputTokens':     max(delta_out, safe_int(old.get('outputTokens', 0))),
        'totalTokens':      max(delta_tot, safe_int(old.get('totalTokens', 0))),
        'cacheReadTokens':  max(delta_cr,  safe_int(old.get('cacheReadTokens', 0))),
        'cacheWriteTokens': max(delta_cw,  safe_int(old.get('cacheWriteTokens', 0))),
        'model':   s.get('model',   'unknown'),
        'agent':   s.get('agent',   'unknown'),
        'channel': s.get('channel', 'unknown'),
        '_ic': max(round(delta_inp * r['input']     / 1000, 8), safe_float(old.get('_ic', 0))),
        '_oc': max(round(delta_out * r['output']    / 1000, 8), safe_float(old.get('_oc', 0))),
        '_rc': max(round(delta_cr  * r['cacheRead'] / 1000, 8), safe_float(old.get('_rc', 0))),
        '_wc': max(round(delta_cw  * r['cacheWrite']/ 1000, 8), safe_float(old.get('_wc', 0))),
    }
    past_entry['_contributions'] = past_contrib
    backfill_count += 1

if backfill_count:
    # Recompute totals for all backfilled entries
    for s_date, past_entry in daily_map_pre.items():
        if s_date == today or not past_entry.get('_contributions'):
            continue
        orig_total = safe_int(past_entry.get('totalTokens', 0))
        if orig_total > 0:
            continue  # has real data, skip
        pe = past_entry
        pe.update({
            'inputTokens': 0, 'outputTokens': 0, 'totalTokens': 0,
            'inputCostUSD': 0.0, 'outputCostUSD': 0.0,
            'cacheReadCostUSD': 0.0, 'cacheWriteCostUSD': 0.0, 'sessions': 0,
            'models': {}, 'agents': {}, 'channels': {},
        })
        for sid, c in past_entry['_contributions'].items():
            inp  = safe_int(c.get('inputTokens', 0))
            out  = safe_int(c.get('outputTokens', 0))
            tot  = safe_int(c.get('totalTokens', 0))
            ic   = safe_float(c.get('_ic', 0))
            oc   = safe_float(c.get('_oc', 0))
            rc   = safe_float(c.get('_rc', 0))
            wc   = safe_float(c.get('_wc', 0))
            m  = c.get('model', 'unknown') or 'unknown'
            a  = c.get('agent', 'unknown') or 'unknown'
            ch = c.get('channel', 'unknown') or 'unknown'
            if tot > 0 or inp > 0:
                pe['sessions'] += 1
            pe['inputTokens']  += inp
            pe['outputTokens'] += out
            pe['totalTokens']  += tot
            pe['inputCostUSD']  += ic
            pe['outputCostUSD'] += oc
            pe['cacheReadCostUSD']  += rc
            pe['cacheWriteCostUSD'] += wc
            for bk, key in ((pe['models'], m), (pe['agents'], a), (pe['channels'], ch)):
                if key not in bk:
                    bk[key] = {'sessions':0,'inputTokens':0,'outputTokens':0,'totalTokens':0,'estimatedCostUSD':0.0}
                bk[key]['sessions']         += 1 if (tot>0 or inp>0) else 0
                bk[key]['inputTokens']      += inp
                bk[key]['outputTokens']     += out
                bk[key]['totalTokens']      += tot
                bk[key]['estimatedCostUSD'] += ic + oc + rc + wc
        pe['cacheReadTokens']  = max(safe_int(pe.get('cacheReadTokens', 0)),
                                     sum(safe_int(c.get('cacheReadTokens',0)) for c in past_entry['_contributions'].values()))
        pe['cacheWriteTokens'] = max(safe_int(pe.get('cacheWriteTokens', 0)),
                                     sum(safe_int(c.get('cacheWriteTokens',0)) for c in past_entry['_contributions'].values()))
        pe['costUSD'] = round(pe['inputCostUSD'] + pe['outputCostUSD'] +
                              pe['cacheReadCostUSD'] + pe['cacheWriteCostUSD'], 8)
        for k in ('inputCostUSD','outputCostUSD','cacheReadCostUSD','cacheWriteCostUSD'):
            pe[k] = round(pe[k], 8)
    print(f"  Backfill: {backfill_count} sessions attributed to historical dates")
    # Merge backfilled entries back into data's daily list
    for s_date, pe in daily_map_pre.items():
        if s_date not in {d['date'] for d in data.get('daily', [])}:
            data.setdefault('daily', []).append(pe)
        else:
            for i, d in enumerate(data['daily']):
                if d['date'] == s_date and safe_int(d.get('totalTokens', 0)) == 0:
                    data['daily'][i] = pe
                    break

# —— 2. INTERVALS ————————————————————————————————————————————————
intervals = data.get('intervals', [])
gw_cr_today = 0
gw_cw_today = 0

if isinstance(gw_status, dict) and gw_status:
    iv_models, iv_agents = {}, {}
    for m in gw_status.get('models', []):
        mn = m.get('name') or m.get('model','unknown')
        mi = safe_int(m.get('inputTokens'))
        mo = safe_int(m.get('outputTokens'))
        mg = safe_int(m.get('totalTokens'))
        mc = safe_float(m.get('estimatedCostUSD'))
        if not mc:
            r = get_rates(mn)
            mc = round((mi*r['input']+mo*r['output'])/1000, 8)
        iv_models[mn] = {'inputTokens':mi, 'outputTokens':mo,
                         'totalTokens':max(mg, mi+mo) if (mi+mo)>0 else mg,
                         'estimatedCostUSD':round(mc, 8)}
    for a in gw_status.get('agents', []):
        an = a.get('name') or a.get('agentName','unknown')
        ai = safe_int(a.get('inputTokens'))
        ao = safe_int(a.get('outputTokens'))
        ag = safe_int(a.get('totalTokens'))
        iv_agents[an] = {'inputTokens':ai, 'outputTokens':ao,
                         'totalTokens':max(ag, ai+ao) if (ai+ao)>0 else ag}
    gi = safe_int(gw_status.get('inputTokens'))
    go = safe_int(gw_status.get('outputTokens'))
    gt = safe_int(gw_status.get('totalTokens'))
    gw_cr_today = safe_int(gw_status.get('cacheReadTokens') or
                           gw_status.get('cache_read_tokens') or
                           gw_status.get('cachedInputTokens'))
    gw_cw_today = safe_int(gw_status.get('cacheWriteTokens') or
                           gw_status.get('cache_write_tokens') or
                           gw_status.get('cacheCreationInputTokens'))
    intervals.append({
        'timestamp': now, 'hour': hour_bucket,
        'inputTokens': gi, 'outputTokens': go,
        'totalTokens': max(gt, gi+go) if (gi+go)>0 else gt,
        'models': iv_models, 'agents': iv_agents,
        'activeSessions': safe_int(gw_status.get('activeSessions') or
                                   gw_status.get('sessions')),
    })

intervals = intervals[-500:]
data['intervals'] = intervals

# —— 3. HOURLY — preserve existing, merge new session data ———————
existing_hourly = {h['hour']: h for h in data.get('hourly', [])}

for s in all_sessions:
    hk = to_hour(s.get('updatedAt',''))
    if not hk: continue
    if hk not in existing_hourly:
        existing_hourly[hk] = {
            'hour': hk, 'inputTokens': 0, 'outputTokens': 0,
            'totalTokens': 0, 'estimatedCostUSD': 0.0,
            'models': {}, 'agents': {}, 'activeSessions': 0, 'intervals': 0,
        }
    b = existing_hourly[hk]
    mk = s.get('model', 'unknown')
    if mk not in b['models']:
        b['models'][mk] = {'inputTokens':0,'outputTokens':0,'totalTokens':0,'estimatedCostUSD':0.0}
    # MAX per model — handles updated sessions without double-counting
    b['models'][mk] = {
        'inputTokens':      max(b['models'][mk]['inputTokens'],      s['inputTokens']),
        'outputTokens':     max(b['models'][mk]['outputTokens'],     s['outputTokens']),
        'totalTokens':      max(b['models'][mk]['totalTokens'],      s['totalTokens']),
        'estimatedCostUSD': max(b['models'][mk]['estimatedCostUSD'], s['estimatedCostUSD']),
    }
    ak = s.get('agent', 'unknown')
    if ak not in b['agents']:
        b['agents'][ak] = {'inputTokens':0,'outputTokens':0,'totalTokens':0}
    b['agents'][ak] = {
        'inputTokens':  max(b['agents'][ak]['inputTokens'],  s['inputTokens']),
        'outputTokens': max(b['agents'][ak]['outputTokens'], s['outputTokens']),
        'totalTokens':  max(b['agents'][ak]['totalTokens'],  s['totalTokens']),
    }

# Recompute top-level hourly fields from sub-buckets so they always add up
for b in existing_hourly.values():
    b['inputTokens']      = sum(m['inputTokens']      for m in b['models'].values())
    b['outputTokens']     = sum(m['outputTokens']      for m in b['models'].values())
    b['totalTokens']      = sum(m['totalTokens']       for m in b['models'].values())
    b['estimatedCostUSD'] = sum(m['estimatedCostUSD']  for m in b['models'].values())

for iv in intervals:
    hk = iv.get('hour','')
    if hk and hk in existing_hourly:
        existing_hourly[hk]['activeSessions'] = max(
            existing_hourly[hk]['activeSessions'], iv.get('activeSessions', 0))
        existing_hourly[hk]['intervals'] += 1

hourly_sorted = sorted(existing_hourly.values(), key=lambda h: h['hour'])
data['hourly'] = []
for h in hourly_sorted[-168:]:
    out = {'hour': h['hour']}
    for fk, fv in h.items():
        if fk == 'hour': continue
        out[fk] = round(fv, 8) if isinstance(fv, float) else fv
    data['hourly'].append(out)

# —— 4. TODAY'S DAILY ENTRY — accumulate via contribution tracking ——
#
# _contributions maps session ID → token/cost snapshot. Persists across
# cron runs so data survives gateway returning empty sessions.

daily_map = {d['date']: d for d in data.get('daily', [])}
today_sessions = [s for s in all_sessions
                  if s.get('date')==today or to_date(s.get('updatedAt',''))==today]
prev = daily_map.get(today, {})
contributions = prev.get('_contributions', {})

# Migration: if prev exists from v7 (no _contributions), seed from its totals
# so accumulated data isn't lost on the first v8 run
if prev and not prev.get('_contributions') and safe_int(prev.get('totalTokens', 0)) > 0:
    contributions['_migrated'] = {
        'inputTokens':      safe_int(prev.get('inputTokens', 0)),
        'outputTokens':     safe_int(prev.get('outputTokens', 0)),
        'totalTokens':      safe_int(prev.get('totalTokens', 0)),
        'cacheReadTokens':  safe_int(prev.get('cacheReadTokens', 0)),
        'cacheWriteTokens': safe_int(prev.get('cacheWriteTokens', 0)),
        'model': 'unknown', 'agent': 'unknown', 'channel': 'unknown',
        '_ic': safe_float(prev.get('inputCostUSD', 0)),
        '_oc': safe_float(prev.get('outputCostUSD', 0)),
        '_rc': safe_float(prev.get('cacheReadCostUSD', 0)),
        '_wc': safe_float(prev.get('cacheWriteCostUSD', 0)),
    }

# Merge current sessions into contributions (take max per field)
for s in today_sessions:
    sid = s.get('id', '')
    if not sid: continue
    old = contributions.get(sid, {})
    contributions[sid] = {
        'inputTokens':      max(s['inputTokens'],                    safe_int(old.get('inputTokens', 0))),
        'outputTokens':     max(s['outputTokens'],                   safe_int(old.get('outputTokens', 0))),
        'totalTokens':      max(s['totalTokens'],                    safe_int(old.get('totalTokens', 0))),
        'cacheReadTokens':  max(s.get('cacheReadTokens', 0),         safe_int(old.get('cacheReadTokens', 0))),
        'cacheWriteTokens': max(s.get('cacheWriteTokens', 0),        safe_int(old.get('cacheWriteTokens', 0))),
        'model':   s.get('model',   old.get('model', 'unknown')),
        'agent':   s.get('agent',   old.get('agent', 'unknown')),
        'channel': s.get('channel', old.get('channel', 'unknown')),
        '_ic': max(s.get('_ic', 0), safe_float(old.get('_ic', 0))),
        '_oc': max(s.get('_oc', 0), safe_float(old.get('_oc', 0))),
        '_rc': max(s.get('_rc', 0), safe_float(old.get('_rc', 0))),
        '_wc': max(s.get('_wc', 0), safe_float(old.get('_wc', 0))),
    }

# Cache tokens: best of session contributions, gateway status, previous entry
prev_cr = safe_int(prev.get('cacheReadTokens', 0))
prev_cw = safe_int(prev.get('cacheWriteTokens', 0))
s_cr = sum(safe_int(c.get('cacheReadTokens', 0))  for c in contributions.values())
s_cw = sum(safe_int(c.get('cacheWriteTokens', 0)) for c in contributions.values())
fcr  = max(s_cr, gw_cr_today, prev_cr)
fcw  = max(s_cw, gw_cw_today, prev_cw)

# Build today's entry from ALL contributions (single pass)
entry = {
    'date': today, 'inputTokens': 0, 'outputTokens': 0,
    'cacheReadTokens': fcr, 'cacheWriteTokens': fcw,
    'totalTokens': 0,
    'inputCostUSD': 0.0, 'outputCostUSD': 0.0,
    'cacheReadCostUSD': 0.0, 'cacheWriteCostUSD': 0.0,
    'sessions': 0, 'models': {}, 'agents': {}, 'channels': {},
    '_contributions': contributions,
}

for sid, c in contributions.items():
    m  = c.get('model',  'unknown') or 'unknown'
    a  = c.get('agent',  'unknown') or 'unknown'
    ch = c.get('channel','unknown') or 'unknown'
    inp  = safe_int(c.get('inputTokens', 0))
    out  = safe_int(c.get('outputTokens', 0))
    tot  = safe_int(c.get('totalTokens', 0))
    ic   = safe_float(c.get('_ic', 0))
    oc   = safe_float(c.get('_oc', 0))
    rc   = safe_float(c.get('_rc', 0))
    wc   = safe_float(c.get('_wc', 0))
    # Use component cost so breakdowns match daily totals
    cost = ic + oc + rc + wc

    has_activity = tot > 0 or inp > 0 or out > 0
    if has_activity:
        entry['sessions'] += 1

    entry['inputTokens']     += inp
    entry['outputTokens']    += out
    entry['totalTokens']     += tot
    entry['inputCostUSD']    += ic
    entry['outputCostUSD']   += oc
    entry['cacheReadCostUSD']+= rc
    entry['cacheWriteCostUSD']+= wc

    for bucket, key in ((entry['models'], m), (entry['agents'], a), (entry['channels'], ch)):
        if key not in bucket:
            bucket[key] = {'sessions':0,'inputTokens':0,'outputTokens':0,
                           'totalTokens':0,'estimatedCostUSD':0.0}
        bucket[key]['sessions']         += 1 if has_activity else 0
        bucket[key]['inputTokens']      += inp
        bucket[key]['outputTokens']     += out
        bucket[key]['totalTokens']      += tot
        bucket[key]['estimatedCostUSD'] += cost

# Handle aggregate cache tokens that aren't tracked per-session
unaccounted_cr = fcr - s_cr
unaccounted_cw = fcw - s_cw
if unaccounted_cr > 0 and entry['cacheReadCostUSD'] == 0:
    prev_cr_cost = safe_float(prev.get('cacheReadCostUSD', 0))
    if prev_cr_cost > 0 and fcr == prev_cr:
        entry['cacheReadCostUSD'] = prev_cr_cost
    else:
        total_w = entry['totalTokens'] or 1
        avg_rate = sum(
            get_rates(c.get('model','unknown'))['cacheRead'] * safe_int(c.get('totalTokens',0))
            for c in contributions.values()
        ) / total_w
        entry['cacheReadCostUSD'] = round(fcr * avg_rate / 1000, 8)

if unaccounted_cw > 0 and entry['cacheWriteCostUSD'] == 0:
    prev_cw_cost = safe_float(prev.get('cacheWriteCostUSD', 0))
    if prev_cw_cost > 0 and fcw == prev_cw:
        entry['cacheWriteCostUSD'] = prev_cw_cost
    else:
        total_w = entry['totalTokens'] or 1
        avg_rate = sum(
            get_rates(c.get('model','unknown'))['cacheWrite'] * safe_int(c.get('totalTokens',0))
            for c in contributions.values()
        ) / total_w
        entry['cacheWriteCostUSD'] = round(fcw * avg_rate / 1000, 8)

# Enforce invariant: costUSD = sum of all cost parts
for k in ('inputCostUSD','outputCostUSD','cacheReadCostUSD','cacheWriteCostUSD'):
    entry[k] = round(entry[k], 8)
entry['costUSD'] = round(entry['inputCostUSD'] + entry['outputCostUSD'] +
                         entry['cacheReadCostUSD'] + entry['cacheWriteCostUSD'], 8)

# Filter ghost entries from breakdowns
for bk in ('models', 'agents', 'channels'):
    entry[bk] = {k: v for k, v in entry[bk].items()
                 if v.get('totalTokens', 0) > 0 or v.get('sessions', 0) > 0}
for v in entry['models'].values():   v['estimatedCostUSD'] = round(v['estimatedCostUSD'], 8)
for v in entry['agents'].values():   v['estimatedCostUSD'] = round(v['estimatedCostUSD'], 8)
for v in entry['channels'].values(): v['estimatedCostUSD'] = round(v['estimatedCostUSD'], 8)

# Verify: sum(model breakdown) == daily totals
model_token_sum = sum(v['totalTokens'] for v in entry['models'].values())
model_cost_sum  = sum(v['estimatedCostUSD'] for v in entry['models'].values())
if model_token_sum != entry['totalTokens'] and entry['models']:
    print(f"  WARN: model totalTokens sum ({model_token_sum}) != daily ({entry['totalTokens']})")
if abs(model_cost_sum - entry['costUSD']) > 0.01 and entry['models']:
    print(f"  WARN: model cost sum ({model_cost_sum:.4f}) != daily ({entry['costUSD']:.4f})")

daily_map[today] = entry

# —— 4b. CLEANUP: strip internal fields, normalize historical data ——
def normalize_model_name(name):
    for prefix in ('google/', 'anthropic/', 'openai/', 'meta/', 'mistral/'):
        if name.startswith(prefix):
            return name[len(prefix):]
    return name

cleaned_daily = []
for d in sorted(daily_map.values(), key=lambda x: x['date']):
    d_date = d.get('date', '')
    if d_date != today:
        d.pop('_contributions', None)
        # Remove past days with truly zero activity (gateway was down, no real data)
        if (safe_int(d.get('totalTokens', 0)) == 0
                and safe_int(d.get('sessions', 0)) == 0
                and safe_float(d.get('costUSD', 0.0)) == 0.0):
            continue
        # Clean ghost zero-token model/agent entries and normalize model name prefixes
        for bk in ('models', 'agents', 'channels'):
            if not d.get(bk):
                continue
            clean = {}
            for raw_name, v in d[bk].items():
                if safe_int(v.get('totalTokens', 0)) == 0 and safe_int(v.get('sessions', 0)) == 0:
                    continue
                norm_name = normalize_model_name(raw_name) if bk == 'models' else raw_name
                if norm_name in clean:
                    # Merge duplicate entries (e.g. 'google/model' + 'model')
                    for fld in ('sessions','inputTokens','outputTokens','totalTokens'):
                        clean[norm_name][fld] = safe_int(clean[norm_name].get(fld,0)) + safe_int(v.get(fld,0))
                    clean[norm_name]['estimatedCostUSD'] = round(
                        safe_float(clean[norm_name].get('estimatedCostUSD',0)) +
                        safe_float(v.get('estimatedCostUSD',0)), 8)
                else:
                    clean[norm_name] = dict(v)
            d[bk] = clean
    cleaned_daily.append(d)
data['daily'] = cleaned_daily[-365:]

# —— 5. MODEL + AGENT AGGREGATES (from daily breakdowns) —————————

def normalize_model_name(name):
    for prefix in ('google/', 'anthropic/', 'openai/', 'meta/', 'mistral/'):
        if name.startswith(prefix):
            return name[len(prefix):]
    return name

INTERNAL_AGENTS = {'gateway/ws', 'gateway', 'unknown', ''}

magg = defaultdict(lambda:{'sessions':0,'inputTokens':0,'outputTokens':0,
                            'totalTokens':0,'estimatedCostUSD':0.0})
aagg = defaultdict(lambda:{'sessions':0,'inputTokens':0,'outputTokens':0,
                            'totalTokens':0,'estimatedCostUSD':0.0})
cagg = defaultdict(lambda:{'sessions':0,'inputTokens':0,'outputTokens':0,
                            'totalTokens':0,'estimatedCostUSD':0.0})

for day in data['daily']:
    for raw_name, v in (day.get('models') or {}).items():
        if not v.get('sessions',0) and not v.get('totalTokens',0): continue
        name = normalize_model_name(raw_name)
        for fld in ('sessions','inputTokens','outputTokens','totalTokens'):
            magg[name][fld] += safe_int(v.get(fld, 0))
        magg[name]['estimatedCostUSD'] += safe_float(v.get('estimatedCostUSD', 0))

    for raw_name, v in (day.get('agents') or {}).items():
        if not v.get('sessions',0) and not v.get('totalTokens',0): continue
        if raw_name in INTERNAL_AGENTS: continue
        for fld in ('sessions','inputTokens','outputTokens','totalTokens'):
            aagg[raw_name][fld] += safe_int(v.get(fld, 0))
        aagg[raw_name]['estimatedCostUSD'] += safe_float(v.get('estimatedCostUSD', 0))

    for ch_name, v in (day.get('channels') or {}).items():
        if not v.get('sessions',0) and not v.get('totalTokens',0): continue
        for fld in ('sessions','inputTokens','outputTokens','totalTokens'):
            cagg[ch_name][fld] += safe_int(v.get(fld, 0))
        cagg[ch_name]['estimatedCostUSD'] += safe_float(v.get('estimatedCostUSD', 0))

def ra(d): return {k: round(v,8) if isinstance(v,float) else v for k,v in d.items()}
def has_data(v): return v.get('totalTokens',0) > 0 or v.get('sessions',0) > 0
data['models']   = {k: ra(v) for k,v in magg.items() if has_data(v)}
data['agents']   = {k: ra(v) for k,v in aagg.items() if has_data(v)}
data['channels'] = {k: ra(v) for k,v in cagg.items() if has_data(v)}

# —— 6. TOTALS (summed from daily — single source of truth) ——————
TOKEN_FIELDS = ('inputTokens','outputTokens','cacheReadTokens','cacheWriteTokens','totalTokens')
COST_FIELDS  = ('costUSD','inputCostUSD','outputCostUSD','cacheReadCostUSD','cacheWriteCostUSD')

t = {k: 0 for k in TOKEN_FIELDS}
t.update({k: 0.0 for k in COST_FIELDS})
t['days'] = len(data['daily'])
for d in data['daily']:
    for k in TOKEN_FIELDS:
        t[k] += safe_int(d.get(k, 0))
    for k in COST_FIELDS:
        t[k] += safe_float(d.get(k, 0.0))
for k in COST_FIELDS:
    t[k] = round(t[k], 8)

data['totals']      = t
data['lastUpdated'] = now

# Verify totals invariant
parts_sum = round(t['inputCostUSD'] + t['outputCostUSD'] +
                  t['cacheReadCostUSD'] + t['cacheWriteCostUSD'], 8)
if abs(parts_sum - t['costUSD']) > 0.01:
    print(f"  WARN: totals costUSD ({t['costUSD']:.4f}) != sum of parts ({parts_sum:.4f})")

# —— 7. WRITE + BACKUP ———————————————————————————————————————————
with open(data_file, 'w') as f: json.dump(data, f, indent=2)
with open(backup_file, 'w') as f: json.dump(data, f, indent=2)

# Update snapshot so next run can compute accurate deltas
new_snap = {
    'timestamp': now,
    'sessions': [
        {
            'id':                s['id'],
            'inputTokens':       s['inputTokens'],
            'outputTokens':      s['outputTokens'],
            'totalTokens':       s['totalTokens'],
            'cacheReadTokens':   s.get('cacheReadTokens', 0),
            'cacheWriteTokens':  s.get('cacheWriteTokens', 0),
            'model':             s.get('model', 'unknown'),
            'agent':             s.get('agent', 'unknown'),
            'updatedAt':         s.get('updatedAt', ''),
        }
        for s in data['sessions']
    ],
}
with open(snapshot_file, 'w') as f: json.dump(new_snap, f, indent=2)

denom = t['inputTokens'] + t['cacheReadTokens']
cache_pct = round(t['cacheReadTokens']/denom*100, 1) if denom > 0 else 0.0
print(f"Updated: {len(data['sessions'])} sess | {len(data['hourly'])} hourly"
      f" | {len(data['daily'])} daily | today={len(contributions)} contribs"
      f" | cache={cache_pct}% | tokens={t['totalTokens']:,} | cost=${t['costUSD']:.4f}")
PYEOF

# —— COMMIT + PUSH ————————————————————————————————————————————————
cd "$REPO_DIR"
git add data/usage.json
if git diff --cached --quiet; then
  echo "No changes to commit."
else
  git commit -m "chore: usage snapshot $NOW"
  git push origin main && echo "Pushed." || {
    echo "Push rejected, retrying..."
    git fetch origin main
    git push --force-with-lease origin main \
      && echo "Pushed." \
      || echo "WARNING: push failed — saved locally, will retry next run."
  }
fi
