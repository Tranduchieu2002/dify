FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY t.py dify_exporter.py

# State file cần ghi được -> mount volume vào /var/lib/dify-exporter
RUN useradd -r -u 10001 -m -d /home/exporter exporter \
 && mkdir -p /var/lib/dify-exporter \
 && chown -R exporter:exporter /var/lib/dify-exporter /app
USER exporter

EXPOSE 9192

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9192/metrics', timeout=4).status==200 else 1)"

ENTRYPOINT ["python3", "-u", "dify_exporter.py"]