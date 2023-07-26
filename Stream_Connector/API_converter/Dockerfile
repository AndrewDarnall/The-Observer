FROM node:latest
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
RUN mkdir ./datastorage
COPY converter.js .
CMD ["node","converter.js"]