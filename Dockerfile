FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY railway_recorder.py .
CMD ["python", "railway_recorder.py"]
