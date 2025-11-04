# Devcontainer for Apache Beam (Python) - DirectRunner
# Base image with Python 3.12 on Debian Bookworm
FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm

# Install system deps commonly needed to build Python wheels and run Beam
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends     build-essential     git     curl     ca-certificates     && rm -rf /var/lib/apt/lists/*

# Create a workspace directory (VS Code Dev Containers mounts into /workspaces by default)
WORKDIR /workspaces
