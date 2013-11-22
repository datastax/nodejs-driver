var fs = require('fs');
var path = require('path');
var utils = require('../lib/utils.js');

var config = {
  "host": "localhost",
  "host2": "localhost",
  "port": 9042,
  "username": "cassandra",
  "password": "cassandra"
};

if (fs.existsSync(path.resolve(__dirname, './localConfig.json'))) {
  var localConfig = require('./localConfig.json');
  utils.extend(config, localConfig);
}

module.exports = config;