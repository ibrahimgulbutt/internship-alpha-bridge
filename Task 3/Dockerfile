# Use Node image
FROM node:20

# Set working directory
WORKDIR /app

# Configure npm for better network resilience
RUN npm config set fetch-retry-mintimeout 20000 && \
    npm config set fetch-retry-maxtimeout 120000 && \
    npm config set fetch-retries 3 && \
    npm config set registry https://registry.npmjs.org/

# Copy backend files
COPY backend/package*.json ./backend/
RUN cd backend && npm install --no-audit --no-fund

# Copy frontend files
COPY frontend/package*.json ./frontend/
RUN cd frontend && npm install --no-audit --no-fund

# Copy rest of the files
COPY backend ./backend
COPY frontend ./frontend

# Expose ports
EXPOSE 3000 5000

# Start both frontend and backend using concurrently
RUN npm install -g concurrently --no-audit --no-fund

CMD ["concurrently", "cd backend && npm run dev", "cd frontend && npm start"]
