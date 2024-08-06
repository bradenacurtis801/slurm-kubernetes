# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /App

# Copy the current directory contents into the container at /App
COPY . /App

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port the app runs on
EXPOSE 3000

# Ensure entrypoint.sh is executable
RUN chmod +x /App/entrypoint.sh

# Run the entrypoint script when the container launches
ENTRYPOINT ["/App/entrypoint.sh"]
