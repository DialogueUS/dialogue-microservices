#!/bin/bash
# Creates the three task queues (+ a DLQ each, maxReceiveCount=3, per
# spec §3) and the document bucket when LocalStack reports ready.
set -euo pipefail

region=us-east-1

make_queue() {
  local name=$1 visibility=$2
  local dlq_url dlq_arn
  dlq_url=$(awslocal sqs create-queue --queue-name "${name}-dlq" --query QueueUrl --output text)
  dlq_arn=$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
  awslocal sqs create-queue --queue-name "$name" --attributes "{
    \"VisibilityTimeout\": \"${visibility}\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${dlq_arn}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"
  }"
}

make_queue harvest-sweep-tasks 300
make_queue harvest-code-tasks 900
make_queue harvest-fetch-tasks 300
awslocal s3 mb "s3://harvest-documents" --region "$region"
echo "harvest queues + bucket ready"
