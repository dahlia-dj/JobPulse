FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py db.py charts.py cv.py ./
COPY .streamlit ./.streamlit
COPY assets ./assets

# Le package silver (catalogue de compétences spaCy, utilisé pour lire les CV) est monté
# depuis airflow/plugins/silver par docker-compose : il n'existe qu'en un seul exemplaire.
ENV PYTHONPATH=/app

EXPOSE 8501
HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
