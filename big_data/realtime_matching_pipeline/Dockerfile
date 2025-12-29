FROM eclipse-temurin:17-jdk

RUN apt-get update && apt-get install -y maven git curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

CMD mvn clean package && tail -f /dev/null
