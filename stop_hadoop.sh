#!/bin/bash
echo "=== Stopping HDFS services ==="
hdfs --daemon stop secondarynamenode
hdfs --daemon stop datanode
hdfs --daemon stop namenode

echo "=== All HDFS daemons stopped ==="