FROM python:3.13.14-slim

WORKDIR /app

COPY requirements/ /requirements/
RUN pip install --no-cache-dir -r /requirements/dashboard.txt

COPY dags/constants.py .
COPY dashboard/app.py .
COPY dashboard/assets/ ./assets/

EXPOSE 8050

CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "2", "app:server"]
