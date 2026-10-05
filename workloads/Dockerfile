FROM python:3.12-slim
WORKDIR /app
COPY workloads/service.py /app/service.py
USER 10001:10001
EXPOSE 8080
CMD ["python", "/app/service.py"]
