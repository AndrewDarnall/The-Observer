# Stream connector Dockerfile
FROM node:latest
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
RUN mkdir ./datastorage
COPY streaming_data_connector.js .
CMD ["node","streaming_data_connector.js"]