import asyncio
import os

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))


TOPICS = [
    {
        "name": "crypto.trades.raw",
        "partitions": 3,
        "configs": {
            "cleanup.policy": "delete",
            "retention.ms": "86400000"
        }
    },
    {
        "name": "crypto.trades.clean",
        "partitions": 3,
        "configs": {
            "cleanup.policy": "delete",
            "retention.ms": "86400000"
        }
    },
    {
        "name": "crypto.metrics",
        "partitions": 3,
        "configs": {
            "cleanup.policy": "delete",
            "retention.ms": "21600000"
        }
    },
    {
        "name": "crypto.alerts",
        "partitions": 3,
        "configs": {
            "cleanup.policy": "delete",
            "retention.ms": "604800000"
        }
    }
]


async def create_topics():
    admin = AIOKafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="crypto-topic-admin"
    )

    await admin.start()

    try:
        existing_topics = await admin.list_topics()
        topics_to_create = []

        for topic in TOPICS:
            if topic["name"] in existing_topics:
                print(f"Topic déjà existant ignoré : {topic['name']}")
                continue

            topics_to_create.append(
                NewTopic(
                    name=topic["name"],
                    num_partitions=topic["partitions"],
                    replication_factor=REPLICATION_FACTOR,
                    topic_configs=topic["configs"]
                )
            )

        if not topics_to_create:
            print("Aucun topic à créer. Tous les topics existent déjà.")
            return

        try:
            await admin.create_topics(
                new_topics=topics_to_create,
                validate_only=False
            )

            for topic in topics_to_create:
                print(f"Topic créé : {topic.name}")

        except TopicAlreadyExistsError:
            print("Certains topics existent déjà.")

    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(create_topics())