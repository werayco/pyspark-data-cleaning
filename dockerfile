FROM python:3.10-slim
RUN apt-get update && apt-get install -y default-jdk-headless && apt-get clean

WORKDIR /app
RUN pip install pyspark==4.0.1 
COPY runner.py .
CMD ["python", "runner.py"]