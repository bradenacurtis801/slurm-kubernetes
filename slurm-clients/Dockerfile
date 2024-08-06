# Use the official CUDA runtime as a parent image
FROM nvidia/cuda:12.2.2-cudnn8-devel-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive

# Set environment variables for NCCL and OpenMPI
ENV NCCL_VERSION=1.3.4-1
ENV NCCL_PACKAGE=libnccl1_${NCCL_VERSION}.cuda8.0_amd64.deb
ENV VERSION=5.0.5
ENV TZ=Etc/UTC
ENV SM_ARCH=sm_86
ENV COMPUTE_CAP=compute_86

# Install necessary packages and dependencies
RUN apt-get update && \
    apt-get install -y \
    zlib1g \
    zlib1g-dev \
    build-essential \
    devscripts \
    debhelper \
    fakeroot \
    git \
    wget \
    nano \
    vim \
    sudo \
    python3 \
    python3-pip \
    python3-venv \
    mariadb-server \
    munge \
    slurmd \
    slurm-client \
    slurmctld \
    openssh-server \
    gnupg \
    curl \
    tzdata \
    software-properties-common \
    pkg-config \
    libcairo2-dev && \
    rm -rf /var/lib/apt/lists/*

# Configure the timezone
RUN ln -fs /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure --frontend noninteractive tzdata

# Clone, build, and install NCCL
RUN git clone https://github.com/NVIDIA/nccl.git && \
    cd nccl && \
    make -j src.build NVCC_GENCODE="-gencode=arch=${COMPUTE_CAP},code=${SM_ARCH}" && \
    make pkg.debian.build && \
    dpkg -i build/pkg/deb/libnccl2_*.deb && \
    dpkg -i build/pkg/deb/libnccl-dev_*.deb && \
    cd .. && \
    rm -rf nccl

# Verify NCCL installation
RUN ldconfig && \
    dpkg -l | grep nccl

# Download and install OpenMPI
RUN wget https://download.open-mpi.org/release/open-mpi/v5.0/openmpi-${VERSION}.tar.gz && \
    tar -xzf openmpi-${VERSION}.tar.gz && \
    cd openmpi-${VERSION} && \
    ./configure --prefix=/usr/local && \
    make all && \
    sudo make install && \
    ldconfig && \
    rm -rf openmpi-${VERSION} openmpi-${VERSION}.tar.gz

# Add a user to manage everything, and be able to ssh (will be handy everywhere)
RUN useradd -m admin -s /bin/bash -d /home/admin && \
    echo "admin:admin" | chpasswd && \
    adduser admin sudo && \
    echo "admin     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Set Munge options
RUN echo 'OPTIONS="--force --key-file /etc/munge/munge.key"' > /etc/default/munge

# Copy entrypoint script
COPY docker-entrypoint.sh /

COPY ./master /App/master/
COPY ./worker /App/worker/

# Copy requirements.txt and install dependencies
COPY requirements.txt /App/requirements.txt
RUN pip install --no-cache-dir -r /App/requirements.txt

# Set permissions for the entrypoint script
RUN chmod +x /docker-entrypoint.sh

# Set environment variables for CUDA, NCCL, and OpenMPI
ENV PATH="/usr/local/cuda/bin:/usr/local/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/cuda/lib64:/usr/local/lib:${LD_LIBRARY_PATH}"

# Define entrypoint
ENTRYPOINT ["/docker-entrypoint.sh"]
