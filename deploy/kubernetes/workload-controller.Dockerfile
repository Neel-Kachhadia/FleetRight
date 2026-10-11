FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY python/fleetright/ /app/fleetright/
ENV PYTHONPATH=/app
USER 10001:10001
ENTRYPOINT ["python", "-m", "fleetright.workload_controller"]
