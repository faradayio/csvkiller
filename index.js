#!/usr/bin/env nodejs

var async = require('async');
var mkdirp = require('mkdirp');
var fs = require('fs');
var child_process = require('child_process');
var cluster = require('cluster');
var numCPUs = require('os').cpus().length;

var program = require('commander');

program
  .version('0.0.1')
  .usage('-c [column] [options] [file ...]')
  .option('-c, --column [name]', 'Which column to segment by')
  .option('-d, --delimiter [delimiter]', 'How to split up lines in the input file [,]', ',')
  .option('-od, --output-delimiter [delimiter]', 'How to split up lines in the output files [,]', ',')
  .option('-b, --buffer-size [characters]', 'Number of characters can be in the in-memory file buffer before it\'s written to the disk [1000000]', parseInt, 1000000)
  .parse(process.env.ARGS ? JSON.parse(process.env.ARGS) : process.argv);

if (!program.column) {
  console.error('Please specify a column');
  program.help();
}
var inputFiles = process.env.FILENAME ? [process.env.FILENAME] : program.args;
if (!inputFiles.length) {
  console.error('Please specify at least one input file');
  process.exit(1);
}

if (program.delimiter == 'TAB') {
  program.delimiter = '\t';
}
if (program.outputDelimiter == 'TAB') {
  program.outputDelimiter = '\t';
}

if (cluster.isMaster) {

  mkdirp.sync('tmp');

  var fileQueue = async.queue(function(fileName, callback){
    console.log('Creating worker for '+fileName);
    var worker = cluster.fork({
      ARGS: JSON.stringify(process.argv),
      FILENAME: fileName
    });
    worker.on('exit', function(code, signal){
      console.log('Worker closed ('+fileName+') code '+code);
      callback();
    });
  }, numCPUs);

  inputFiles.forEach(function(fileName){
    fileQueue.push(fileName);
  });

  fileQueue.drain = function(){
    child_process.spawn('bash', [__dirname+'/combiner.sh'], { stdio: 'inherit' });
  };
} else {
  var outputStreams = {};
  var outputBuffers = {};
  var writeFile = function(dirName, columnNames, name, data){
    if (typeof outputStreams[name] == 'undefined') {
      outputStreams[name] = fs.createWriteStream(dirName+'/'+name+'.csv');
      outputBuffers[name] = '';
      outputBuffers[name] += columnNames.join(program.outputDelimiter)+'\n';
    }
    outputBuffers[name] += data.join(program.outputDelimiter)+'\n';
    if (outputBuffers[name].length > 1000000) {
      outputStreams[name].write(outputBuffers[name]);
      outputBuffers[name] = '';
    }
  };

  async.each(inputFiles, function(inputFile, done){
    mkdirp.sync('tmp/'+inputFile);

    var i = 0;
    var columnNames;

    var targetIndex;

    var stream = fs.createReadStream(inputFile, 'LATIN1');
    var workingBuffer = '';
    stream.on('data', function(chunk){
      workingBuffer += chunk;
      var lines = workingBuffer.split('\n');
      while (lines.length > 1) {
        var line = lines.shift();
        var data = line.split(program.delimiter);

        if (i === 0) {
          columnNames = data;
          targetIndex = columnNames.indexOf(program.column);
          if (targetIndex == -1) {
            throw new Error('Column "'+program.column+'" not found');
          }
        } else {
          writeFile('tmp/'+inputFile, columnNames, data[targetIndex], data);
        }
        i++;
      }
      workingBuffer = lines[0];
    });

    stream.on('end', function(){
      done();
    });
  }, function(err){
    for (var name in outputStreams) {
      if (outputBuffers[name].length > 0) {
        outputStreams[name].write(outputBuffers[name]);
        outputBuffers[name] = '';
      }
      outputStreams[name].end();
    }
    if (err) {
      console.error(err);
    }
    process.exit(0);
  });
}