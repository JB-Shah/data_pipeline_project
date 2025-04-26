FROM bitnami/spark:latest

# Install Python and pip
USER root
RUN apt-get update && apt-get install -y python3 python3-pip

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY streaming_app.py .

# Switch back to Spark user
USER 1001
