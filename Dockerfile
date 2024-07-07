# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the pyproject.toml and the README.md into the container at /app
COPY pyproject.toml README.md /app/

# Copy the .env file into the container
COPY .env /app/

# Install dependencies
RUN pip install --upgrade pip \
    && pip install setuptools \
    && pip install --no-cache-dir -r <(python -c "import toml; print('\n'.join(toml.load('pyproject.toml')['dependencies']))")

# Copy the rest of the application code into the container
COPY . /app

# Expose the port your app runs on. Flask by default runs on port 5000.
EXPOSE 5000

# Define environment variable
ENV FLASK_APP=app.py
ENV FLASK_ENV=production

# Command to run the application
CMD ["flask", "run", "--host=0.0.0.0"]
