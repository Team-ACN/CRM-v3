# ── Stage 1: Next.js build (Node only) ───────────────────────────────
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build
# Prune to production deps only
RUN npm prune --omit=dev

# ── Stage 2: Runtime (Python 3.11 + Node 20) ─────────────────────────
FROM python:3.11-slim
RUN apt-get update && apt-get install -y curl gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy built Next.js app + pruned node_modules
COPY --from=builder /app/.next      ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/public     ./public
COPY package*.json ./

# Python scripts
COPY scripts/ ./scripts/

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PYTHON_BIN=python3

EXPOSE 3000
CMD ["node", "node_modules/.bin/next", "start"]
