#!/bin/bash
set -e
echo "[hdfs-init] Waiting for namenode RPC port 9000..."
until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; do
  sleep 5
done
echo "[hdfs-init] Namenode ready, uploading static datasets..."
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /data/language_rankings.csv /data/language_rankings.csv
echo "[hdfs-init] Static datasets uploaded to HDFS"
hdfs dfs -ls /data
