FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Root app uses flask_app.py
CMD ["python", "flask_app.py", "--host", "0.0.0.0", "--port", "5000"]
