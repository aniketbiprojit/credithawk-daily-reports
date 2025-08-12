# Use official Node.js 20 Alpine base image
FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY . .

# Build TypeScript code
RUN npm run build

# Expose the port the app runs on
EXPOSE 8080

# Start the server
CMD ["node", "dist/index.js"]
