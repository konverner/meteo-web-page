# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the pyproject.toml and the README.md into the container at /app
COPY pyproject.toml README.md /app/

# Copy the .env file into the container
COPY .env /app/

# Install build dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install optional dependencies if needed
RUN pip install --no-cache-dir .

# Copy the rest of the application code into the container
COPY . /app

# Expose the port your app runs on. Flask by default runs on port 5000.
EXPOSE 5000

# Define environment variable
ENV FLASK_APP=main.py
ENV FLASK_ENV=production

# Command to run the application
CMD ["python", "src/meteo_web_page/main.py"]
