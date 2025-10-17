FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Cài Scala + sbt + tiện ích
RUN apt-get update && apt-get install -y curl gnupg software-properties-common

# Cài Scala
RUN curl -fsSL https://www.scala-lang.org/files/archive/scala-2.12.18.deb -o scala.deb && \
    apt-get install -y ./scala.deb && \
    rm scala.deb

# Cài sbt (Scala build tool)
RUN curl -fsSL https://repo.scala-sbt.org/scalasbt/debian/sbt-1.9.8.deb -o sbt.deb && \
    apt-get install -y ./sbt.deb && \
    rm sbt.deb


# Cài thêm gói cần thiết
RUN pip install pymongo
RUN pip install findspark
RUN pip install pyspark
RUN pip install kafka-python
RUN pip install pyarrow
RUN pip install pandas

# (tuỳ chọn) tải Kafka connector .jar nếu không có internet trong container
# RUN mkdir -p /usr/local/spark/jars && \
#     wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar -O /usr/local/spark/jars/spark-sql-kafka.jar && \
#     wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar -O /usr/local/spark/jars/kafka-clients.jar

WORKDIR /home/notebooks
