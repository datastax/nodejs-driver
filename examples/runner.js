"use strict";

var async = require('async');
var exec = require('child_process').exec;
var fs = require('fs');

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

var runnerFileName = './' + module.filename.split('/').reduce(function (p, c) { return c;});
var counter = 0;
async.eachSeries(getJsFiles('./'), function (file, next) {
  if (file === runnerFileName) return next();
  exec('node ' + file, function (err) {
    counter++;
    next(err);
  });
}, function (err) {
  if (err) {
    console.error(err);
    return;
  }
  console.log('%d examples executed successfully', counter);
});
