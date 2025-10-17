FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Cài thêm gói cần thiết
RUN pip install findspark
RUN pip install pyspark
RUN pip install kafka-python
RUN pip install pyarrow

# (tuỳ chọn) tải Kafka connector .jar nếu không có internet trong container
# RUN mkdir -p /usr/local/spark/jars && \
#     wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar -O /usr/local/spark/jars/spark-sql-kafka.jar && \
#     wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar -O /usr/local/spark/jars/kafka-clients.jar

WORKDIR /home/notebooks
