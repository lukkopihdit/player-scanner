FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY bot.py /app/bot.py

RUN mkdir -p /data

CMD ["python", "-u", "/app/bot.py"]
