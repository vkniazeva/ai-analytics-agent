#!/bin/bash
set -e

echo "Initializing Superset..."

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password "${SUPERSET_ADMIN_PASSWORD}" \
  || true

superset init

ASSETS_DIR="/app/assets"

if [ -d "$ASSETS_DIR/databases" ] || [ -d "$ASSETS_DIR/datasets" ] || \
   [ -d "$ASSETS_DIR/charts" ] || [ -d "$ASSETS_DIR/dashboards" ]; then

  echo "Starting Superset in background for asset import..."
  superset run -h 0.0.0.0 -p 8088 &
  SUPERSET_PID=$!

  echo "Waiting for Superset to be ready..."
  for i in $(seq 1 30); do
    if curl -s http://localhost:8088/health > /dev/null 2>&1; then
      echo "Superset is ready."
      break
    fi
    sleep 2
  done

  echo "Importing assets..."
  python -c "
import zipfile, os, requests

assets = '/app/assets'
zip_path = '/tmp/superset_assets.zip'

with zipfile.ZipFile(zip_path, 'w') as zf:
    for root, dirs, files in os.walk(assets):
        for f in files:
            if f == '.gitkeep':
                continue
            full = os.path.join(root, f)
            arcname = 'dashboard_export/' + os.path.relpath(full, assets)
            info = zipfile.ZipInfo(arcname, date_time=(2026, 1, 1, 0, 0, 0))
            with open(full, 'rb') as fh:
                zf.writestr(info, fh.read())

base_url = 'http://localhost:8088'

login = requests.post(f'{base_url}/api/v1/security/login', json={
    'username': os.environ['SUPERSET_ADMIN_USER'],
    'password': os.environ['SUPERSET_ADMIN_PASSWORD'],
    'provider': 'db',
})
access_token = login.json()['access_token']
headers = {'Authorization': f'Bearer {access_token}'}

csrf = requests.get(f'{base_url}/api/v1/security/csrf_token/', headers=headers)
csrf_token = csrf.json()['result']
headers['X-CSRFToken'] = csrf_token
headers['Referer'] = base_url

import json, yaml

db_password_map = {}
db_dir = os.path.join(assets, 'databases')
if os.path.isdir(db_dir):
    for fname in os.listdir(db_dir):
        fpath = os.path.join(db_dir, fname)
        with open(fpath) as yf:
            db_conf = yaml.safe_load(yf)
            uri = db_conf.get('sqlalchemy_uri', '')
            # Extract password from URI
            if '@' in uri and ':' in uri.split('@')[0]:
                userpass = uri.split('://')[1].split('@')[0]
                password = userpass.split(':', 1)[1]
                db_password_map[f'databases/{fname}'] = password

with open(zip_path, 'rb') as f:
    resp = requests.post(
        f'{base_url}/api/v1/dashboard/import/',
        headers=headers,
        files={'formData': ('assets.zip', f, 'application/zip')},
        data={'overwrite': 'true', 'passwords': json.dumps(db_password_map)},
    )
    if resp.status_code == 200:
        print('Dashboard import successful!')
    else:
        print(f'Import failed ({resp.status_code}): {resp.text}')
"

  echo "Assets import complete."
  kill $SUPERSET_PID 2>/dev/null
  wait $SUPERSET_PID 2>/dev/null || true
fi

echo "Superset initialization complete."
