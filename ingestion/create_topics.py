from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = "localhost:29092"
TOPICS = ["npm-packages", "github-events"]

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="riskgraph-admin")
existing = set(admin.list_topics())
new_topics = [NewTopic(name=t, num_partitions=2, replication_factor=1)
              for t in TOPICS if t not in existing]
if new_topics:
    admin.create_topics(new_topics=new_topics, validate_only=False)
print("Kafka topics:", admin.list_topics())
admin.close()
