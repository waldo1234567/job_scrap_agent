FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates wget curl gnupg git \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . .
ENV PORT=7860

EXPOSE ${PORT}

CMD ["python", "dashboard.py"]