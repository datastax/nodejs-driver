"use strict";

const async = require('async');
const exec = require('child_process').exec;
const fs = require('fs');
const path = require('path');

/**
 * This script is used to check that the samples run correctly.
 * It is not a valid example, see README.md and subdirectories for more information.
 */

/** List all js files in the directory */
function getJsFiles(dir, fileArray) {
  const files = fs.readdirSync(dir);
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

if (+process.versions.node.split('.')[0] < 10) {
  throw new Error('Examples were not executed as they were designed to run against Node.js 10+');
}

const runnerFileName = path.basename(module.filename);
let counter = 0;
let failures = 0;

async.eachSeries(getJsFiles(path.dirname(module.filename) + path.sep), function (file, next) {
  if (file.indexOf(runnerFileName) >= 0) {
    return next();
  }

  let timedOut = false;
  const timeout = setTimeout(function() {
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