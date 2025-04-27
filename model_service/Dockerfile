FROM python:3.11.11-slim-bullseye

WORKDIR /usr/src/app

COPY requirements.txt ./
USER root

RUN apt update && apt install -y \
    build-essential \
    python3-dev \
    libffi-dev \
    vim \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python", "main.py"]