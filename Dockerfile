# Small official Python image (Debian-based, minus extras we don't need)
FROM python:3.12-slim

# Don't write .pyc files; print logs immediately instead of buffering them
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy requirements first so Docker caches the install layer;
# editing producer.py later won't trigger a reinstall.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Run as an unprivileged user rather than root
USER nobody

CMD ["python", "producer.py"]
