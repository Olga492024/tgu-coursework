FROM python:3.9-slim
WORKDIR /app
COPY . .
RUN pip install --default-timeout=1000 -r requirements.txt
CMD ["python", "model.py"]
