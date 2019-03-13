#!/bin/bash

set -e

export AWS_ACCESS_KEY_ID="fnordzzz"
export AWS_SECRET_ACCESS_KEY="fnordzzz"
export AWS_S3_ENDPOINT="http://minio:9002"
export AWS_S3_BUCKET_NAME="fnordzzz"
export AWS_SQS_QUEUE_NAME="local-queue"

echo "Creating minio bucket"
# don't let the "bucket already created errors" stop the process
aws --endpoint $AWS_S3_ENDPOINT s3api create-bucket --bucket $AWS_S3_BUCKET_NAME || true

set -vx
exec "$@"
echo "done"
