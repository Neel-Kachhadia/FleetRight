FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY fleetright/workload_controller.py /app/workload_controller.py
USER 10001:10001
ENTRYPOINT ["python", "/app/workload_controller.py"]
