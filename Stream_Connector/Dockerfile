# Stream API connector service container
FROM python:3.9
WORKDIR /app
COPY . /app
RUN mkdir /app/data
RUN pip install --no-cache httpx
CMD [ "python3", "stream_api_connector.py" ]