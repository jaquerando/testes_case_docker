# Use the Python 3.9 slim image as the base
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file for the specific DAG
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all other files for the DAG
COPY . .

# Expose port 8080 for the server
EXPOSE 8080

# Set the default command to start the FastAPI server
CMD ["uvicorn", "bronze:app", "--host", "0.0.0.0", "--port", "8080"]




