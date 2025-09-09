# ---- Stage 1: Build ----
# This stage compiles the TypeScript code into JavaScript
FROM node:18 as builder
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# ---- Stage 2: Run ----
# This stage creates the final, lean image for production
FROM node:18-slim
# Install FFmpeg
RUN apt-get update && apt-get install -y ffmpeg
WORKDIR /usr/src/app
# Copy only production dependencies
COPY package*.json ./
RUN npm install --production
# Copy the compiled JavaScript from the 'builder' stage
COPY --from=builder /usr/src/app/dist ./dist
# Command to run the compiled application
CMD ["node", "dist/index.js"]