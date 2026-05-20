FROM flink:1.18-scala_2.12

# Установка Python3, pip и JDK development files
RUN apt-get update && \
    apt-get install -y python3 python3-pip openjdk-11-jdk-headless wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Скачивание Kafka коннектора и клиента
RUN wget -O /opt/flink/lib/flink-connector-kafka-3.0.2-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.2-1.18/flink-connector-kafka-3.0.2-1.18.jar && \
    wget -O /opt/flink/lib/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# Установка Python-зависимостей
COPY requirements.txt /opt/flink/job/
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
RUN pip3 install --no-cache-dir -r /opt/flink/job/requirements.txt

# Копирование Flink-джобы
COPY flink_streaming_job.py /opt/flink/job/

ENV PYTHON_EXECUTABLE=/usr/bin/python3

WORKDIR /opt/flink/job