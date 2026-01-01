FROM eclipse-temurin:17-jdk

RUN apt-get update && apt-get install -y maven git curl python3 python3-pip && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements & cài Python deps
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

CMD ["bash", "-c", "mvn clean package && python3 ./src/main/python/API.py"]
