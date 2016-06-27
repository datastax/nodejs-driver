"use strict";

var async = require('async');
var exec = require('child_process').exec;
var fs = require('fs');
var path = require('path');

/**
 * This script is used to check that the samples run correctly.
 * It is not a valid examples, see README.md and subdirectories for more information.
 */

/** List all js files in the directory */
function getJsFiles(dir, fileArray) {
  var files = fs.readdirSync(dir);
  fileArray = fileArray || [];
  files.forEach(function(file) {
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

var runnerFileName = path.basename(module.filename);
var counter = 0;
async.eachSeries(getJsFiles(path.dirname(module.filename) + path.sep), function (file, next) {
  if (file.indexOf(runnerFileName) >= 0 || file.indexOf('node_modules') >= 0) {
    return next();
  }
  exec('node ' + file, function (err) {
    counter++;
    process.stdout.write('.');
    if (err) {
      console.log('Failed %s', file);
    }
    next(err);
  });
}, function (err) {
  if (err) {
    console.error(err);
    return;
  }
  console.log('\n%d examples executed successfully', counter);
});
