# CARPARK Backfill

A tool to backfill a given bucket destination by reading from a source bucket all its content. While doing so, side indexes will be generated.

## Usage

Drop a `.env` file in the project root and populate:

```sh
# Origin Bucket
ORIGIN_BUCKET_REGION=<value>
ORIGIN_BUCKET_NAME=<value>
ORIGIN_BUCKET_ENDPOINT=<value>
ORIGIN_BUCKET_ACCESS_KEY_ID=<value>
ORIGIN_BUCKET_SECRET_ACCESS_KEY=<value>

# Destination Bucket
DESTINATION_BUCKET_REGION=<value>
DESTINATION_BUCKET_NAME=<value>
DESTINATION_BUCKET_ENDPOINT=<value>
DESTINATION_BUCKET_ACCESS_KEY_ID=<value>
DESTINATION_BUCKET_SECRET_ACCESS_KEY=<value>

# Destination Bucket for side indexes
DESTINATION_SIDE_INDEX_BUCKET_REGION=<value>
DESTINATION_SIDE_INDEX_BUCKET_NAME=<value>
DESTINATION_SIDE_INDEX_BUCKET_ENDPOINT=<value>
DESTINATION_SIDE_INDEX_BUCKET_ACCESS_KEY_ID=<value>
DESTINATION_SIDE_INDEX_BUCKET_SECRET_ACCESS_KEY=<value>

# Destination Bucket for root CID mapping
DESTINATION_ROOT_INDEX_BUCKET_REGION=<value>
DESTINATION_ROOT_INDEX_BUCKET_NAME=<value>
DESTINATION_ROOT_INDEX_BUCKET_ENDPOINT=<value>
DESTINATION_ROOT_INDEX_BUCKET_ACCESS_KEY_ID=<value>
DESTINATION_ROOT_INDEX_BUCKET_SECRET_ACCESS_KEY=<value>
```

Start the tool:

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