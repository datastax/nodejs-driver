'use strict';

class Result {
  constructor(rs) {
    this._rs = rs;
  }

  first() {
    this._rs.first();
  }
}

module.exports = Result;