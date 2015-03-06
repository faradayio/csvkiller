#!/usr/bin/env nodejs

var async = require('async');
var mkdirp = require('mkdirp');
var fs = require('fs');
var path = require('path');
var child_process = require('child_process');
var cluster = require('cluster');
var csv = require('fast-csv');
var os = require('os');
var numCPUs = os.cpus().length;

var program = require('commander');

var package = JSON.parse(fs.readFileSync(__dirname+'/package.json', 'utf8'));

program
  .version(package.version)
  .usage('-c [column] [options] [file ...]')
  .option('-c, --column [name]', 'Which column to segment by')
  .option('-d, --delimiter [delimiter]', 'How to split up lines in the input file (use TAB for tab-delimited) [,]', ',')
  .option('-o, --output-directory [path]', 'Output directory [./output]', './output')
  .option('-od, --output-delimiter [delimiter]', 'How to split up lines in the output files (use TAB for tab-delimited) [,]', ',')
  .option('-b, --buffer-size [characters]', 'Max characters in the output buffer [1000000]', parseInt, 1000000)
  .option('-u, --uppercase', 'Case insensitive column matching, write to OUTPUT.csv instead of Output.csv')
  .option('-l, --lowercase', 'Case insensitive column matching, write to output.csv instead of Output.csv')
  .option('-t, --truncate [length]', 'Truncate file names to the first X number of characters', parseInt, 0)
  .option('-v, --verbose', 'Verbose output')
  .parse(process.env.ARGS ? JSON.parse(process.env.ARGS) : process.argv);

var logVerbose = function(){
  if (program.verbose) {
    console.log.apply(console, arguments);
  }
};

var randomDirectoryName = function(){
  //something like csvkiller_wwjzky9z8xx8yqfr
  return 'csvkiller_'+Math.random().toString(36).substr(2);
};

var makeTempDirectory = function(){
  var parentDirectory = os.tmpdir();

  var tmpDirectory = parentDirectory+'/'+randomDirectoryName();

  mkdirp.sync(tmpDirectory);

  return tmpDirectory;
};

if (!program.outputDirectory) {
  console.error('Invalid output directory');
  program.help();
}

program.outputDirectory = path.resolve(program.outputDirectory);

if (typeof process.env.__CSVKILLER_TMP_DIR == 'undefined') {
  program.tmpDirectory = makeTempDirectory();
} else {
  program.tmpDirectory = process.env.__CSVKILLER_TMP_DIR;
}

mkdirp.sync(program.outputDirectory);

if (program.uppercase && program.lowercase) {
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

if (inputFiles.length == 1 && !process.env.FILENAME) {
  var columnNames, targetIndex;

  var outputStreams = {};

  var stream = csv.fromPath(inputFiles[0], {delimiter: program.delimiter});

  var i = 0;
  stream.on('record', function(data){
    if (i == 0) {
      columnNames = data;
      targetIndex = columnNames.indexOf(program.column);

      if (targetIndex == -1) {
        throw new Error('Column "'+program.column+'" not found');
      }

      logVerbose(inputFiles[0], 'using column', program.column, '(#'+targetIndex+')');
    } else {
      var targetCell = data[targetIndex];
      if (typeof targetCell !== 'undefined') {
        if (program.uppercase) {
          targetCell = targetCell.toUpperCase();
        } else if (program.lowercase) {
          targetCell = targetCell.toLowerCase();
        }
        if (program.truncate) {
          targetCell = targetCell.substr(0, program.truncate);
        }
      }

      if (typeof outputStreams[targetCell] == 'undefined') {
        outputStreams[targetCell] = csv.createWriteStream();
        outputStreams[targetCell].pipe(fs.createWriteStream(program.outputDirectory+'/'+targetCell+'.csv'));
        outputStreams[targetCell].write(columnNames);
      }

      outputStreams[targetCell].write(data);
    }
    i++;
  });

  stream.on('end', function(){
    for (var key in outputStreams) {
      outputStreams[key].end();
    }
  });
} else if (cluster.isMaster) {

  var fileQueue = async.queue(function(fileName, callback){
    console.log('Creating worker for '+fileName);
    var worker = cluster.fork({
      ARGS: JSON.stringify(process.argv),
      __CSVKILLER_TMP_DIR: program.tmpDirectory,
      FILENAME: fileName
    });
    worker.on('exit', function(code, signal){
      console.log('Worker closed ('+fileName+') code '+code);
      if (code != 0) {
        process.exit(1);
      }
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
        if (typeof targetCell !== 'undefined') {
          if (program.uppercase) {
            targetCell = targetCell.toUpperCase();
          } else if (program.lowercase) {
            targetCell = targetCell.toLowerCase();
          }
          if (program.truncate) {
            targetCell = targetCell.substr(0, program.truncate);
          }
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
