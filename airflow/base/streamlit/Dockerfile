FROM python:3.10-slim

WORKDIR /app

# Install Streamlit + libs
COPY streamlit/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy your streamlit source code
COPY projects/absa_streaming/streamlit /app/streamlit

EXPOSE 8501

CMD ["streamlit", "run", "streamlit/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
