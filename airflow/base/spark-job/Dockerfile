FROM eclipse-temurin:21-jdk

# Install Python 3.11 & dependencies
# 1. Cài software-properties-common để thêm PPA
# 2. Thêm PPA deadsnakes (kho chứa các bản Python cũ/mới cho Ubuntu)
# 3. Cài Python 3.11, venv, distutils và dev (quan trọng để build thư viện)
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common wget curl \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-venv \
    python3.11-distutils \
    python3.11-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Tạo venv và upgrade pip
RUN python3.11 -m venv /opt/venv && \
    /opt/venv/bin/pip install --upgrade pip setuptools wheel

ENV PATH="/opt/venv/bin:$PATH"
ENV PYSPARK_PYTHON=/opt/venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

# Install Spark
ARG SPARK_VERSION=3.5.2
ARG HADOOP_VERSION=3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENT_VERSION=3.5.1
ARG POSTGRES_VERSION=42.6.0

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

WORKDIR $SPARK_HOME

RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" --strip-components=1 \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p $SPARK_HOME/jars

WORKDIR $SPARK_HOME/jars

RUN \
    wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka_${SCALA_VERSION}/${KAFKA_CLIENT_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_CLIENT_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar"

WORKDIR /opt/spark-jobs

# ===== Install Python dependencies =====
COPY base/spark-job/requirements.txt /tmp/spark_requirements.txt
RUN pip install --no-cache-dir -r /tmp/spark_requirements.txt
RUN pip install torch==2.3.0 --index-url https://download.pytorch.org/whl/cpu

# ===== Patch kafka.vendor.six.moves lỗi thần thánh =====
# after pip install in venv
# ensure six and patch kafka package imports robustly using python
RUN /opt/venv/bin/pip install --no-cache-dir six || true && \
    /opt/venv/bin/python - <<'PY' || true
import importlib, pathlib, sys, pkgutil
patched = []
# Try direct module location
try:
    import kafka
    base = pathlib.Path(kafka.__file__).parent
    # target file
    p = base / "codec.py"
    if p.exists():
        txt = p.read_text()
        new = txt.replace("from kafka.vendor.six.moves import range", "from six.moves import range")
        new = new.replace("from kafka.vendor.six import", "from six import")
        if new != txt:
            p.write_text(new)
            print("Patched", p)
            patched.append(str(p))
except Exception as e:
    pass

# also scan site-packages inside venv for any other references
import glob, re
site_libs = list(glob.glob("/opt/venv/lib/python*/site-packages"))
for lib in site_libs:
    import os
    for root,_,files in os.walk(lib):
        for f in files:
            if f.endswith(".py"):
                fp = os.path.join(root,f)
                s = open(fp, "r", encoding="utf-8", errors="ignore").read()
                s2 = s.replace("from kafka.vendor.six.moves import range", "from six.moves import range")
                s2 = s2.replace("kafka.vendor.six", "six")
                if s2 != s:
                    open(fp,"w", encoding="utf-8").write(s2)
                    patched.append(fp)
if patched:
    print("Patched files count:", len(patched))
else:
    print("No files patched")
PY


CMD ["sleep", "infinity"]
