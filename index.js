#!/usr/bin/env nodejs

var async = require('async');
var mkdirp = require('mkdirp');
var fs = require('fs');
var child_process = require('child_process');
var cluster = require('cluster');
var csv = require('fast-csv');
var numCPUs = require('os').cpus().length;

var program = require('commander');

var package = JSON.parse(fs.readFileSync('package.json', 'utf8'));

program
  .version(package.version)
  .usage('-c [column] [options] [file ...]')
  .option('-c, --column [name]', 'Which column to segment by')
  .option('-d, --delimiter [delimiter]', 'How to split up lines in the input file (use TAB for tab-delimited) [,]', ',')
  .option('-od, --output-delimiter [delimiter]', 'How to split up lines in the output files (use TAB for tab-delimited) [,]', ',')
  .option('-b, --buffer-size [characters]', 'Max characters in the output buffer [1000000]', parseInt, 1000000)
  .option('--case-sensitivity [uppercase|lowercase]', 'Case handling of the segmentation column [none]', 'none')
  .parse(process.env.ARGS ? JSON.parse(process.env.ARGS) : process.argv);

if (!program.caseSensitivity) {
  program.caseSensitivity = 'none';
}
if (program.caseSensitivity === true) {
  program.caseSensitivity = 'uppercase';
}
if (program.caseSensitivity != 'uppercase' && program.caseSensitivity != 'lowercase' && program.caseSensitivity != 'none') {
  console.error('Invalid case sensitivity flag. Use uppercase, lowercase, none, or simply leave it out.');
  program.help();
}

if (!program.column) {
  console.error('Please specify a column');
  program.help();
}
var inputFiles = process.env.FILENAME ? [process.env.FILENAME] : program.args;
if (!inputFiles.length) {
  console.error('Please specify at least one input file');
  process.exit(1);
}

if (program.delimiter.toLowerCase() == 'tab') {
  program.delimiter = '\t';
}
if (program.outputDelimiter.toLowerCase() == 'tab') {
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
      outputBuffers[name] += csv.writeToString([columnNames], {delimiter: program.outputDelimiter});
    }
    outputBuffers[name] += '\n'+csv.writeToString([data], {delimiter: program.outputDelimiter});
    if (outputBuffers[name].length > 1000000) {
      outputStreams[name].write(outputBuffers[name]);
      outputBuffers[name] = '';
    }
  };

  async.each(inputFiles, function(inputFile, done){
    mkdirp.sync('tmp/'+inputFile);

    var columnNames;

    var targetIndex;

    var stream = csv.fromPath(inputFile, {delimiter: program.delimiter});

    var i = 0;
    stream.on('record', function(data){
      if (i == 0) {
        columnNames = data;
        targetIndex = columnNames.indexOf(program.column);
        if (targetIndex == -1) {
          throw new Error('Column "'+program.column+'" not found');
        }
      } else {
        writeFile('tmp/'+inputFile, columnNames, data[targetIndex], data);
      }
      i++;
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
    cluster.worker.disconnect();
  });
}