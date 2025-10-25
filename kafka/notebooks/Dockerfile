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
COPY requirement.txt .

RUN apt-get update && apt-get install -y build-essential python3-dev
RUN pip install --upgrade pip setuptools wheel cython
RUN pip install -r requirement.txt

WORKDIR /home/notebooks
