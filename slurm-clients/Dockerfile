# Use the official CUDA runtime as a parent image
FROM bradenacurtis801/slurm:oompi-nccl_cuda12.2-cudnn8-ubuntu20.04-sm86-base

# Copy entrypoint script
COPY docker-entrypoint.sh /

COPY ./master /App/master/
COPY ./worker /App/worker/

# Copy requirements.txt and install dependencies
COPY requirements.txt /App/requirements.txt
RUN pip install --no-cache-dir -r /App/requirements.txt

# Set permissions for the entrypoint script
RUN chmod +x /docker-entrypoint.sh

# Define entrypoint
ENTRYPOINT ["/docker-entrypoint.sh"]
