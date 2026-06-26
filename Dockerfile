# Flask + PySpark + Delta；需 JVM 供 Spark 使用
# 對齊 datalake-vm：Python 3.12
FROM python:3.12-slim-bookworm

# BuildKit 會帶入；舊版 builder 可能未設定，預設 amd64
ARG TARGETARCH=amd64
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH} \
    PATH="/usr/lib/jvm/java-17-openjdk-${TARGETARCH}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        tesseract-ocr \
        tesseract-ocr-chi-tra \
        tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-lock.txt requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements-lock.txt

COPY app.py config.py ./
COPY services ./services
COPY templates ./templates
COPY dic ./dic
COPY manifests ./manifests

EXPOSE 5000

ENV PORT=5000
CMD ["python", "-u", "app.py"]
