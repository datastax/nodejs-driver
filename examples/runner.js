"use strict";

var async = require('async');
var exec = require('child_process').exec;
var fs = require('fs');
var path = require('path');

/**
 * This script is used to check that the samples run correctly.
 * It is not a valid example, see README.md and subdirectories for more information.
 */

/** List all js files in the directory */
function getJsFiles(dir, fileArray) {
  var files = fs.readdirSync(dir);
  fileArray = fileArray || [];
  files.forEach(function(file) {
    if (file === 'node_modules') {
      return;
    }
    if (fs.statSync(dir + file).isDirectory()) {
      getJsFiles(dir + file + '/', fileArray);
      return;
    }
    if (file.substring(file.length-3, file.length) !== '.js') {
      return;
    }
    fileArray.push(dir+file);
  });
  return fileArray;
}

if (typeof Promise === 'undefined' || process.version.indexOf('v0.') === 0) {
  console.log('Examples where not executed as a modern runtime is required');
  return;
}

var runnerFileName = path.basename(module.filename);
var counter = 0;
var failures = 0;
async.eachSeries(getJsFiles(path.dirname(module.filename) + path.sep), function (file, next) {
  if (file.indexOf(runnerFileName) >= 0) {
    return next();
  }

  var timedOut = false;
  var timeout = setTimeout(function() {
    console.log("%s timed out after 10s", file);
    counter++;
    failures++;
    next();
  }, 10000);
  exec('node ' + file, function (err) {
    if(timedOut) {
      return;
    }
    counter++;
    clearTimeout(timeout);
    process.stdout.write('.');
    if (err) {
      console.log('Failed %s', file);
      console.error(err);
      failures++;
    }
    next();
  });
}, function (err) {
  if (err) {
    console.error(err);
  }
  console.log('\n%d/%d examples executed successfully', (counter-failures), counter);
  process.exit(failures);
});
