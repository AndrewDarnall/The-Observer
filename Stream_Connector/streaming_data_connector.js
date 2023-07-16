// Streaming data connector in js because it just makes sense when handling JSON data (JS is more flexible with JSON)

const request = require('request');
const fs = require('fs');

const currentDate = new Date();
const str_currentDate = currentDate.toString();
const filePath = './datastorage/mastodon_data_.jsonl';
const fileStream = fs.createWriteStream(filePath);

request.get('https://mastodon.uno/api/v1/streaming/public')
  .on('response', response => {

    // Only for debugging
    // console.log('Response headers:', response.headers);

    // Pipe the response stream to a writable stream (e.g., file, process.stdout)
    response.pipe(fileStream);
    
  })
  .on('error', error => {
    console.error('Error:', error);
  });