#!/bin/bash

BUCKET=$1
DIR=verify_metadata.json
ALERT=false
MSG=""

for f in $(gsutil ls gs://${BUCKET}/output/metadata/${DIR}/*); do
  content=$(gsutil cat "$f")

  count=$(echo "$content" | jq length)

  for i in $(seq 0 $((count - 1))); do
    format=$(echo "$content" | jq -r ".[$i].format")
    status=$(echo "$content" | jq -r ".[$i].status")
    duration=$(echo "$content" | jq -r ".[$i].duration_sec // 0")
    rows=$(echo "$content" | jq -r ".[$i].row_count // 1")

    if [[ "$status" != "success" || $(echo "$duration > 300" | bc) -eq 1 || "$rows" -lt 100 ]]; then
      ALERT=true
      MSG+="Format $format: status=$status, duration=${duration}s, row_count=$rows\n"
    fi
  done
done

if $ALERT; then
  echo -e "$MSG" > /tmp/alert.txt
else
  echo "NO_ALERT" > /tmp/alert.txt
fi