# Use a base image that matches your backend server's requirements
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Mount backend folder into container
VOLUME [":/app"]

# Copy the backend server code into the container
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set any necessary environment variables
ENV PORT=5001

# Expose the port your backend server listens on
EXPOSE $PORT

# Start the backend server
CMD ["python3", "main.py"]
