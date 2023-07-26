const fs = require('fs');
const readline = require('readline');

const filename = './datastorage/raw_data_.jsonl';

// Write Stream
const filePath = './datastorage/mastodon_data_.jsonl';
const wrStream = fs.createWriteStream(filePath, { flags: 'a', autoClose: false });

// Read Stream
const fileStream = fs.createReadStream(filename, 'utf8');


let rl = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity 
});


function processLine(line) {
  console.log('Line:', line);

  if(line.includes("{")) {
    var sub = line.substr(6);
    wrStream.write(sub + '\n', 'utf8',() => {});
  } else {
    console.log("False");
  }
  
}

rl.on('line', processLine);

fs.watchFile(filename, (curr, prev) => {
  
  if (curr.size > prev.size) {
    rl.close();
    const newRl = readline.createInterface({
      input: fs.createReadStream(filename, 'utf8'),
      crlfDelay: Infinity 
    });
    newRl.on('line', processLine);
    rl = newRl;
  }
});


rl.on('close', () => {
  console.log('End of batch.');
});
