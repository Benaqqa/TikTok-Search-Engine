# Multi-stage build: Node for Kafka consumer + Python for yt-dlp
FROM node:18-slim as node-stage

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY consumer.js .

# Second stage: Python with yt-dlp
FROM python:3.11-slim

# Install Node.js runtime + ffmpeg (required for audio extraction)
RUN apt-get update && apt-get install -y curl ffmpeg && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install yt-dlp with curl-cffi for TikTok impersonation
RUN pip3 install --break-system-packages "yt-dlp[default,curl-cffi]"

# Copy Node.js artifacts from node stage
COPY --from=node-stage /app/node_modules ./node_modules
COPY --from=node-stage /app/consumer.js .
COPY --from=node-stage /app/package*.json ./

RUN mkdir -p /output

ENV NODE_PATH=/app/node_modules

CMD ["node", "consumer.js"]