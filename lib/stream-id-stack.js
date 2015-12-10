"use strict";

/**
 * Group size
 * @type {number}
 */
var groupSize = 128;
/**
 * Number used to right shift ids to allocate them into groups
 * @const
 * @type {number}
 */
var shiftToGroup = 7;
/**
 * Amount of groups that can be released per time
 * If it grows larger than 4 groups (128 * 4), groups can be released
 * @const
 * @type {number}
 */
var releasableSize = 4;
/**
 * 32K possible stream ids depending for protocol v3 and above
 * @const
 * @type {number}
 */
var maxGroupsFor2Bytes = 256;
/**
 * Delay used to check if groups can be released
 * @const
 * @type {number}
 */
var releaseDelay = 5000;
/**
 * Represents a queue of ids from 0 to maximum stream id supported by the protocol version.
 * Clients can dequeue a stream id using {@link StreamIdStack#shift()} and enqueue (release) using {@link StreamIdStack#push()}
 * @param {Number} version Protocol version
 * @constructor
 */
function StreamIdStack(version) {
  //Ecmascript Number is 64-bit double, it can be optimized by the engine into a 32-bit int, but nothing below that.
  //We try to allocate as few as possible in arrays of 128
  this.currentGroup = generateGroup(0);
  this.groupIndex = 0;
  this.groups = [this.currentGroup];
  this.releaseTimeout = null;
  this.setVersion(version);
  /**
   * Returns the amount of ids currently in use
   * @member {number}
   */
  this.inUse = 0;
}

/**
 * Sets the protocol version
 * @param {Number} version
 */
StreamIdStack.prototype.setVersion = function (version) {
  //128 or 32K stream ids depending on the protocol version
  this.maxGroups = version < 3 ? 1 : maxGroupsFor2Bytes;
};

/**
 * Dequeues an id.
 * Similar to {@link Array#pop()}.
 * @returns {Number} Returns an id or null
 */
StreamIdStack.prototype.pop = function () {
  var id = this.currentGroup.pop();
  if (typeof id !== 'undefined') {
    this.inUse++;
    return id;
  }
  //try to use the following groups
  while (this.groupIndex < this.groups.length - 1) {
    //move to the following group
    this.currentGroup = this.groups[++this.groupIndex];
    //try dequeue
    id = this.currentGroup.pop();
    if (typeof id !== 'undefined') {
      this.inUse++;
      return id;
    }
  }
  return this._tryCreateGroup();
};
/**
 * Enqueue an id for future use.
 * Similar to {@link Array#push()}.
 * @param {Number} id
 */
StreamIdStack.prototype.push = function (id) {
  this.inUse--;
  var groupIndex = id >> shiftToGroup;
  var group = this.groups[groupIndex];
  group.push(id);
  if (groupIndex < this.groupIndex) {
    //Set the lower group to be used to dequeue from
    this.groupIndex = groupIndex;
    this.currentGroup = group;
  }
  this._tryIssueRelease();
};

/**
 * Clears all timers
 */
StreamIdStack.prototype.clear = function () {
  if (this.releaseTimeout) {
    clearTimeout(this.releaseTimeout);
    this.releaseTimeout = null;
  }
};

/**
 * Tries to create an additional group and returns a new id
 * @returns {Number} Returns a new id or null if it's not possible to create a new group
 * @private
 */
StreamIdStack.prototype._tryCreateGroup = function () {
  if (this.groups.length === this.maxGroups) {
    //we can have an additional group
    return null;
  }
  //Add a new group at the last position
  this.groupIndex = this.groups.length;
  //Using 128 * groupIndex as initial value
  this.currentGroup = generateGroup(this.groupIndex << shiftToGroup);
  this.groups.push(this.currentGroup);
  this.inUse++;
  return this.currentGroup.pop();
};

StreamIdStack.prototype._tryIssueRelease = function () {
  if (this.releaseTimeout || this.groups.length <= releasableSize) {
    //Nothing to release or a release delay has been issued
    return;
  }
  var self = this;
  this.releaseTimeout = setTimeout(function () {
    self._releaseGroups();
  }, releaseDelay);
};

StreamIdStack.prototype._releaseGroups = function () {
  var counter = 0;
  var index = this.groups.length - 1;
  //only release up to n groups (n = releasable size)
  //shrink back up to n groups not all the way up to 1
  while (counter++ < releasableSize && this.groups.length > releasableSize && index > this.groupIndex) {
    if (this.groups[index].length !== groupSize) {
      //the group is being used
      break;
    }
    this.groups.pop();
    index--;
  }
  this.releaseTimeout = null;
  //Issue next release if applies
  this._tryIssueRelease();
};

function generateGroup(initialValue) {
  var arr = new Array(groupSize);
  var upperBound = initialValue + groupSize - 1;
  for (var i = 0; i < groupSize; i++) {
    arr[i] = upperBound - i;
  }
  return arr;
}

module.exports = StreamIdStack;