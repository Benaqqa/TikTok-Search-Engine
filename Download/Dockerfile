# Multi-stage build: Node for Kafka consumer + Python for yt-dlp
FROM node:18-slim as node-stage

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY consumer.js .

# Second stage: Python with yt-dlp
FROM python:3.11-slim

# Install Node.js runtime for consumer
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install yt-dlp
RUN pip install --no-cache-dir yt-dlp

# Copy Node.js artifacts from node stage
COPY --from=node-stage /app/node_modules ./node_modules
COPY --from=node-stage /app/consumer.js .
COPY --from=node-stage /app/package*.json ./

# Create output directory
RUN mkdir -p /output

# Set environment
ENV NODE_PATH=/app/node_modules

# Run the consumer
CMD ["node", "consumer.js"]

