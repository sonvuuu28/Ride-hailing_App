pipeline 1: Cách set up
- pipeline_app_container sẽ là nơi có mvn và java để build pipeline và đưa về .jar
- mount file jar vào flink jobmanager để submit, check UI
- pom.xml là requirements trong python
- mvn clean package
- flink run -c com.example.App -m flink-jobmanager:9081 /app/target/my-flink-job-1.0-SNAPSHOT.jar
- Tra pom trên mvn repo