# MSPR Platform (Flask + React + Postgres)

## Quick Start

- Start stack with Docker Compose:

```bash
make compose-up
```

- Check API health:

```bash
make health
```

- Initialize DB and load seed data (dev/testing):

```bash
make initdb
make seed
```

- Import a GTFS feed by URL (replace with your feed):

```bash
make import GTFS_URL=https://example.com/google_transit.zip
```

## Tests

- Backend (pytest):

```bash
make backend-test
```

- Frontend (Jest):

```bash
make frontend-test
```

## CI

GitHub Actions runs backend and frontend tests on push/PR: see `.github/workflows/ci.yml`.