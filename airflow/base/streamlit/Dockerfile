# Base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ENABLE_CORS=false

# Set working directory
WORKDIR /app

# Copy requirements file
COPY base/streamlit/requirements.txt .

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && apt-get remove -y build-essential \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Copy the rest of your application
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Ensure scripts have execute permissions
RUN find /app -type f -name "*.sh" -exec chmod +x {} \;

# Command to run Streamlit app
CMD ["streamlit", "run", "app.py"]
