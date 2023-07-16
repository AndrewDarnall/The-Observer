// Streaming data connector in js because it just makes sense when handling JSON data

const request = require('request');
const fs = require('fs');

const currentDate = new Date();
const str_currentDate = currentDate.toString();

const filePath = './mastodon_data_' + str_currentDate;

// Required to accept a stream of data
const fileStream = fs.createWriteStream(filePath);

// Make an HTTP GET request with streaming response
request.get('https://mastodon.uno/api/v1/streaming/public')
  .on('response', response => {
    // Only for debugging
    // console.log('Response headers:', response.headers);

    // Pipe the response stream to a writable stream (e.g., file, process.stdout)
    response.pipe(fileStream);
    // Once you do the pipe, simply watch as the new data 'magically' appears on the stream file
  })
  .on('error', error => {
    console.error('Error:', error);
  });