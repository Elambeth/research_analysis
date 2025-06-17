FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements_cloud.txt .
RUN pip install --no-cache-dir -r requirements_cloud.txt

# Copy source code
COPY src/ ./src/
COPY main_cloud.py .
COPY .env .

# Create directories
RUN mkdir -p data/completed data/failed data/progress data/queue logs

# Set environment
ENV PYTHONPATH=/app

# Run the application
CMD ["python", "main_cloud.py"]