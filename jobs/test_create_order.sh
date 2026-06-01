#!/bin/bash

set -e

N=1

while [[ $# -gt 0 ]]; do
   case $1 in
      --N)
         N="$2"
         shift 2
         ;;
      *)
         echo "Unknown argument: $1"
         exit 1
         ;;
   esac
done

random_coord() {
   local center=$1
   local delta=0.02

   awk -v c="$center" -v r="$RANDOM" -v d="$delta" '
   BEGIN {
      srand(r);
      print c + (rand()*2-1)*d
   }'
}

for ((i=1; i<=N; i++))
do
   passenger_id="u$(printf "%05d" $RANDOM)"

   pickup_lat=$(random_coord 37.770000)
   pickup_lng=$(random_coord -122.410000)

   dropoff_lat=$(random_coord 37.780000)
   dropoff_lng=$(random_coord -122.400000)

   pickup_location="${pickup_lat},${pickup_lng}"
   dropoff_location="${dropoff_lat},${dropoff_lng}"

   echo "[${i}/${N}] Creating order for ${passenger_id} from ${pickup_location} to ${dropoff_location}"

   curl -s -X POST http://localhost:8000/api/v1/orders \
      -H "Content-Type: application/json" \
      -d "{
            \"passenger_id\":\"${passenger_id}\",
            \"pickup_location\":\"${pickup_location}\",
            \"dropoff_location\":\"${dropoff_location}\"
      }"
   echo

done
