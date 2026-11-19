# FROM apache/airflow:2.9.3-python3.11

# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# USER airflow
# COPY requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt



FROM apache/airflow:2.9.3-python3.11

# --- 1. Paquets système (root) : compilation, wget, Java pour Spark ----------
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# JAVA_HOME indépendant de l'architecture (amd64 / arm64)
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

# Pilote JDBC PostgreSQL : permet à Spark d'écrire la couche Gold dans PostgreSQL
RUN mkdir -p /opt/spark-jars \
    && wget -q -O /opt/spark-jars/postgresql-42.7.4.jar \
       https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
ENV GOLD_JDBC_JAR=/opt/spark-jars/postgresql-42.7.4.jar

# --- 2. Playwright (paquet Python) : pip doit tourner en utilisateur airflow --
# L'image Airflow refuse « pip install » en root : on installe donc le paquet
# en airflow (il va dans /home/airflow/.local).
USER airflow
RUN pip install --no-cache-dir playwright==1.47.0

# --- 3. Bibliothèques système de Chromium : apt, donc root ------------------
# root ne voit pas les paquets de /home/airflow/.local : on lui indique le chemin.
USER root
RUN PYTHONPATH=/home/airflow/.local/lib/python3.11/site-packages \
    python -m playwright install-deps chromium \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# --- 4. Dépendances du projet + navigateur Chromium (utilisateur airflow) ----
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    && python -m playwright install chromium