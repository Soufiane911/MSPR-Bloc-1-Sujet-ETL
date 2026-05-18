# Continuous delivery (Docker images)

GitHub Actions workflow [`.github/workflows/docker-cd.yml`](../.github/workflows/docker-cd.yml) builds and publishes four images aligned with `docker-compose.yml`:

| Image suffix   | Build context | Compose service   |
|----------------|---------------|-------------------|
| `api`          | `api/`        | `api`             |
| `frontend`     | `frontend/`   | `dashboard` (React, port 8501) |
| `etl`          | `etl/`        | `etl` (profile)   |
| `dashboard`    | `dashboard/`  | `dashboard_dash` (Dash, port 8050) |

## Triggers

- **Push** to `6-3-main`: build, push to GHCR with tags `latest` and commit SHA.
- **Pull request** targeting `6-3-main`: build only (no push), with GHA layer cache.
- Runs in **parallel** with [`.github/workflows/python-tests.yml`](../.github/workflows/python-tests.yml) on the same branch (tests are not a hard gate for image build).

## Registry tags

Images are published under:

`ghcr.io/<owner>/<repo>/<component>:<tag>`

Example: `ghcr.io/soufiane911/mspr-bloc-1-sujet-etl/api:latest`

## Required secrets and permissions

| Name | Purpose |
|------|---------|
| `GITHUB_TOKEN` | Supplied automatically by GitHub Actions. Used to log in to `ghcr.io` and push packages. Workflow job needs `packages: write`. |

No extra registry username/password is required when using GHCR with the default `GITHUB_TOKEN`.

For a private registry other than GHCR, add repository secrets (e.g. `DOCKER_REGISTRY_USER`, `DOCKER_REGISTRY_TOKEN`) and adjust the login step in `docker-cd.yml`.

## Repository settings

1. **Actions**: enable workflows for the repository.
2. **Packages**: after the first successful push, images appear under the repo’s **Packages** tab on GitHub.
3. If the repo is private, grant consumers access to the GHCR packages or use a PAT with `read:packages`.

## Local parity

`docker compose build` uses the same Dockerfiles and contexts as CI (`api`, `frontend` for the React dashboard, `etl`, `dashboard` for Dash).
