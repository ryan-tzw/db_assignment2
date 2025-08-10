#!/bin/bash
echo "=== Starting HDFS services ==="
hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs --daemon start secondarynamenode

echo "=== Waiting for SafeMode to exit (up to 60s) ==="
if ! timeout 60s hdfs dfsadmin -safemode wait; then
    echo "SafeMode still ON after 60s; forcing leave"
    hdfs dfsadmin -safemode leave || true
fi

echo "=== Checking HDFS status ==="
hdfs dfsadmin -report
hdfs dfs -ls /