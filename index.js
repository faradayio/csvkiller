#!/usr/bin/env nodejs

var async = require('async');
var mkdirp = require('mkdirp');
var fs = require('fs');
var path = require('path');
var child_process = require('child_process');
var cluster = require('cluster');
var csv = require('fast-csv');
var numCPUs = require('os').cpus().length;

var program = require('commander');

var package = JSON.parse(fs.readFileSync(__dirname+'/package.json', 'utf8'));

program
  .version(package.version)
  .usage('-c [column] [options] [file ...]')
  .option('-c, --column [name]', 'Which column to segment by')
  .option('-d, --delimiter [delimiter]', 'How to split up lines in the input file (use TAB for tab-delimited) [,]', ',')
  .option('-o, --output-directory [path]', 'Output directory [./output]', './output')
  .option('-t, --tmp-directory [path]', 'Temporary file directory [./tmp]', './tmp')
  .option('-od, --output-delimiter [delimiter]', 'How to split up lines in the output files (use TAB for tab-delimited) [,]', ',')
  .option('-b, --buffer-size [characters]', 'Max characters in the output buffer [1000000]', parseInt, 1000000)
  .option('-u, --uppercase', 'Case insensitive column matching, write to OUTPUT.csv instead of Output.csv')
  .option('-l, --lowercase', 'Case insensitive column matching, write to output.csv instead of Output.csv')
  .option('-v, --verbose', 'Verbose output')
  .parse(process.env.ARGS ? JSON.parse(process.env.ARGS) : process.argv);

var logVerbose = function(){
  if (program.verbose) {
    console.log.apply(console, arguments);
  }
};

if (!program.outputDirectory) {
  console.error('Invalid output directory');
  program.help();
}
if (!program.tmpDirectory) {
  console.error('Invalid tmp directory');
  program.help();
}

program.outputDirectory = path.resolve(program.outputDirectory);
program.tmpDirectory = path.resolve(program.tmpDirectory);

mkdirp.sync(program.tmpDirectory);
mkdirp.sync(program.outputDirectory);

if (program.columnUppercase && program.columnLowercase) {
  console.error('I can\'t upcase and downcase the segmentation column at the same time. Pick one.');
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
    fileQueue.push(path.resolve(fileName));
  });

  fileQueue.drain = function(){
    child_process.spawn('bash', [__dirname+'/combiner.sh', program.tmpDirectory, program.outputDirectory], { stdio: 'inherit' });
  };
} else {
  var outputStreams = {};
  var outputBuffers = {};
  var writeFile = function(dirName, columnNames, name, data){
    if (typeof outputStreams[name] == 'undefined') {
      logVerbose('opening write stream '+dirName+'/'+name+'.csv');
      outputStreams[name] = fs.createWriteStream(dirName+'/'+name+'.csv');
      outputBuffers[name] = '';
      outputBuffers[name] += csv.writeToString([columnNames], {delimiter: program.outputDelimiter});
    }
    logVerbose('writing line to '+name+'.csv', data);
    outputBuffers[name] += '\n'+csv.writeToString([data], {delimiter: program.outputDelimiter});
    if (outputBuffers[name].length > 1000000) {
      outputStreams[name].write(outputBuffers[name]);
      outputBuffers[name] = '';
    }
  };

  async.each(inputFiles, function(inputFile, done){
    mkdirp.sync(program.tmpDirectory+'/'+path.basename(inputFile));

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

        logVerbose(inputFile, 'using column', program.column, '(#'+targetIndex+')');
      } else {
        var targetCell = data[targetIndex];
        if (program.columnUppercase) {
          targetCell = targetCell.toUpperCase();
        } else if (program.columnLowercase) {
          targetCell = targetCell.toLowerCase();
        }
        writeFile(program.tmpDirectory+'/'+path.basename(inputFile), columnNames, targetCell, data);
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
