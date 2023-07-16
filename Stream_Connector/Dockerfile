# Stream connector Dockerfile
FROM node:latest
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
RUN mkdir ./datastorage
COPY streaming_data_connector.js .
CMD ["node","streaming_data_connector.js"]

# Fix the code because something is not working properly
# sudo docker run --rm -v datastorage:/datastorage --network tap_project --name streamConnector -it tap_project:stream_connector
# sudo docker run -v datastorage:/datastorage -it ubuntu (for debugging)