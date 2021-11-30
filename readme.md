1. Build project: `mvn clean package`
   
2. Start docker containers: `docker-compose up -d`

3. execute command to create topic in Kafka with 3 partition:
   `docker exec -it broker /bin/kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic EPAM-Secret-Messages`
   
4. Run Producer application: `producer/com.epam.homework5.task6.Producer.main()`
5. Run Consumer application: `consumer/com.epam.homework5.task6.Consumer.main()`