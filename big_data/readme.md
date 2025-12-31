pipeline 1: Cách set up
- pipeline_app_container sẽ là nơi có mvn và java để build pipeline và đưa về .jar
- mount file jar vào flink jobmanager để submit, check UI
- pom.xml là requirements trong python
- Tra pom trên mvn repo


docker exec -it test_container bash
mvn clean package


docker exec -it flink-jobmanager bash
flink run -c App -m flink-jobmanager:9081 /app/target/my-flink-job-1.0-SNAPSHOT.jar


docker exec -it broker bash
kafka-topics --bootstrap-server broker:29092 --create --if-not-exists --topic gps-topic --partitions 1 --replication-factor 1
kafka-console-producer --topic gps-topic --bootstrap-server broker:29092



docker exec -it redis redis-cli
AUTH 123
HGETALL driver:gps

