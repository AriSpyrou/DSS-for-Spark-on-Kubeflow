#!/bin/sh

SECRET="$(microk8s kubectl get secret | awk 'NR==2{print $1}')"

TOKEN="$(microk8s kubectl get secret $SECRET -o jsonpath='{$.data.token}' | base64 -d)"

curl -k -XGET  -H "Accept: application/json, */*" -H "User-Agent: kubectl/v1.21.13 (linux/amd64) kubernetes/cbc10c9" -H "Authorization: Bearer $TOKEN" 'https://127.0.0.1:16443/api/v1/namespaces/spark-operator/pods/'$1 > /dev/null

curl -k -XGET  -H "User-Agent: kubectl/v1.21.13 (linux/amd64) kubernetes/cbc10c9" -H "Authorization: Bearer $TOKEN" -H "Accept: application/json, */*" 'https://127.0.0.1:16443/api/v1/namespaces/spark-operator/pods/'$1'/log?container=spark-kubernetes-driver'

while true; do OUT="$(microk8s kubectl get -n spark-operator sparkapp adult-preprocess-t9nx8 -o json | jq '.status.applicationState.state')"; if [ "$OUT" = "\"RUNNING"\" ]; then echo "SparkApp still Running"; sleep 5; else break; fi; done