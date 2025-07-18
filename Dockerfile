# Start from slim Python base
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create folders for data and output if not exist
RUN mkdir -p data/raw data/processed data/reports

# Default command: run pipeline
CMD ["python", "main.py"]
