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

# --- public-records pipeline (NEW_PUBLIC_RECORDS.md §2.1) ------------------
# Four standard queues, visibility 900 s, per-queue DLQs:
# maxReceiveCount 3 (inbound, search) / 5 (sender queues).
pr_queue() {
  local name=$1 max_receive=$2
  local dlq_url dlq_arn
  dlq_url=$(awslocal sqs create-queue --queue-name "${name}-dlq" --query QueueUrl --output text)
  dlq_arn=$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
  awslocal sqs create-queue --queue-name "$name" --attributes "{
    \"VisibilityTimeout\": \"900\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${dlq_arn}\\\",\\\"maxReceiveCount\\\":\\\"${max_receive}\\\"}\"
  }"
}

pr_queue pr-search-queries 3
pr_queue pr-contacts 5
pr_queue pr-followups 5
pr_queue pr-inbound-mail 3
awslocal s3 mb "s3://pr-mail" --region "$region"
awslocal s3 mb "s3://pr-documents" --region "$region"
echo "public-records queues + buckets ready"
