# run all tests
uv run --env-file .env pytest tests/ -v

# exclude mlx
# uv run --env-file .env pytest tests/ -v --ignore=tests/test_clip_mlx.py


# specific test file
# uv run --env-file .env pytest tests/test_app.py -v

# specific test
# uv run --env-file .env pytest tests/test_app.py::test_build_score_view_empty -v
