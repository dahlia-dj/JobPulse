# FROM apache/airflow:2.9.3-python3.11

# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# USER airflow
# COPY requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt



FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Playwright a besoin de bibliothèques système pour Chromium (installation
# root obligatoire pour apt-get). L'image officielle Airflow bloque
# volontairement `pip install` en root (garde-fou : message "You are
# running pip as root") -- `python -m pip` contourne ce blocage, car c'est
# le binaire `pip` lui-même qui est intercepté, pas le module. On installe
# donc le package une première fois ici, juste pour disposer de la
# commande `playwright install-deps`.
RUN python -m pip install --no-cache-dir playwright==1.47.0 \
    && python -m playwright install-deps chromium \
    && rm -rf /root/.cache/ms-playwright

# JAVA_HOME indépendant de l'architecture (amd64 / arm64)
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java


USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    && python -m playwright install chromium
