# One image for every Python participant on the mesh (simulator, agency feeds,
# alerting, fusion, consumers). Which one runs is the compose `command`.
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sim/ sim/
COPY services/ services/
ENV PYTHONUNBUFFERED=1
CMD ["python3", "services/alerting.py"]
