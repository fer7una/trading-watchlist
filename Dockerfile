FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY config/ /app/config/
COPY README.md /app/README.md

ENV PYTHONPATH=/app/src

CMD ["python", "scripts/build_watchlist.py"]
