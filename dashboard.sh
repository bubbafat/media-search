set -e

# Start Postgres, set DATABASE_URL, wait for ready, run migrations (reusable)
. ./pg.sh

# if the service is running on 8000, kill it hard
lsof -ti :8000 | xargs kill -9

uv run uvicorn src.api.main:app --reload &

# wait for server to respond
until curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8000/dashboard | grep -q 200; do sleep 0.5; done

# open the dashboard
open http://127.0.0.1:8000/dashboard

fg

