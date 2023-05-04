# CARPARK Backfill

A tool to backfill a given bucket destination by reading from a source bucket all its content. While doing so, side indexes will be generated.

## Usage

1. Copy `.env.tpl` to `.env` file in the project root and populate it as needed.
2. Start the tool:

```sh
npm start
```

Metrics for reports are available at `http://localhost:3000/metrics`

### Docker

There's a `Dockerfile` that runs the tool in docker.

```sh
docker build -t carpark-backfill .
docker run -d carpark-backfill
```