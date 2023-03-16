#!/bin/bash

read -p "manufacturer: " manufacturer
read -p "access_token: " access_token

curl -X POST http://localhost:8080/message/push?topic=company\&entity_id=test -H 'Content-Type: application/json' -d "{\"name\":\"${user}\",\"manufacturer\":\"${manufacturer}\"}" -v
