CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name) 
TOPIC="flight-records"
if kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --list | grep -q "^${TOPIC}$"; then
  kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --topic $TOPIC
  echo "Deleted topic ${TOPIC}."
else
  echo "Topic ${TOPIC} does not exist."
fi
kafka-topics.sh --create --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --replication-factor 1 --partitions 1 --topic $TOPIC