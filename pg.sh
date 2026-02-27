set -e

docker compose up -d
docker compose ps

export DATABASE_URL="postgresql+psycopg2://media_search:media_search@localhost:5432/media_search"

# wait for pg to be up
until docker compose exec postgres pg_isready -U media_search -d media_search; do sleep 1; done

# run migrations
uv run alembic upgrade head

