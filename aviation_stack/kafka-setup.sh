docker compose -f docker-compose.kafka.yml up -d --remove-orphans
docker exec -it kafka kafka-topics --create --topic aviation.flights --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
