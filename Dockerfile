# Use Ubuntu 16.04 as the base image
FROM ubuntu:16.04

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install required packages
RUN apt-get update && \
    apt-get install -y \
        gcc \
        g++ \
        build-essential \
        libopenmpi-dev \
        openmpi-bin \
        default-jdk \
        cmake \
        zlib1g-dev \
        git \
        python \
    && apt-get clean

# Default command
CMD ["/bin/bash"]