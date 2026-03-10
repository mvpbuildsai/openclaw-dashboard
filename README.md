# OpenClaw Usage Dashboard

Live token usage, model analytics, and agent activity dashboard for OpenClaw agents.

## Setup

### 1. Make the collector executable
```bash
chmod +x ~/openclaw-dashboard/scripts/collect.sh
```

### 2. Run manually to test
```bash
~/openclaw-dashboard/scripts/collect.sh
```

### 3. Schedule via cron (every 30 min)
```bash
crontab -e
```
Add this line:
```
*/30 * * * * /home/mvpbuildsai/openclaw-dashboard/scripts/collect.sh >> /home/mvpbuildsai/openclaw-dashboard/collect.log 2>&1
```

### 4. Enable GitHub Pages
- Repo Settings → Pages → Branch: main, folder: / (root)
- Live at: https://mvpbuildsai.github.io/openclaw-dashboard

## Data Sources
- `/tmp/openclaw/*.log` — OpenClaw gateway logs
- `http://127.0.0.1:18789/api/status` — Live gateway stats

## Metrics
- Daily interactions sparkline
- Model usage bar chart
- Agent activity list
- Context window gauge
- Activity feed
