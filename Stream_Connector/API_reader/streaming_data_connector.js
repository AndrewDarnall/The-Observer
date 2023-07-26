// Streaming data connector in js because it just makes sense when handling JSON data (JS is more flexible with JSON)

const request = require('request');
const fs = require('fs');

const currentDate = new Date();
const str_currentDate = currentDate.toString();
const filePath = './datastorage/raw_data_.jsonl';
const fileStream = fs.createWriteStream(filePath);

// https://mastodon.uno/api/v1/streaming/public

request.get(process.env.API).json()
  .on('response', response => {

    // Only for debugging
    // console.log('Response headers:', response.headers);

    // Piping the response to a writeStream file
    response.pipe(fileStream);
    
  })
  .on('error', error => {
    console.error('Error:', error);
  });