
FROM python:3.8-slim-buster
WORKDIR /app/analysis

COPY requirements.txt /app/analysis/requirements.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && apt-get purge -y \
    && apt-get autoremove --purge -y

COPY . /app/analysis

CMD ['python', 'main.py']