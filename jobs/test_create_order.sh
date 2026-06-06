#!/bin/bash

while [[ $# -gt 0 ]]; do
  case $1 in
    --N)
      count="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [ -z "$count" ]; then
  echo "Usage: $0 <number_of_orders>"
  exit 1
fi

echo -e "Create $count orders...\n"

for ((i=1; i<=count; i++)); do

  curl -X POST "http://localhost:8000/orders" \
    -H "Content-Type: application/json" \
    -d '{
      "order_type": "ride",
      "payload": {
        "origin": "台北車站",
        "destination": "松山機場",
        "origin_lat": 25.0478,
        "origin_lng": 121.5170,
        "destination_lat": 25.0630,
        "destination_lng": 121.5530,
        "ride_type": "standard"
      }
    }'
  echo

  sleep 1
done