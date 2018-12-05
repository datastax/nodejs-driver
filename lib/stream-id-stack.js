"use strict";

const types = require('./types');

/**
 * Group size
 * @type {number}
 */
const groupSize = 128;

/**
 * Number used to right shift ids to allocate them into groups
 * @const
 * @type {number}
 */
const shiftToGroup = 7;

/**
 * Amount of groups that can be released per time
 * If it grows larger than 4 groups (128 * 4), groups can be released
 * @const
 * @type {number}
 */
const releasableSize = 4;

/**
 * 32K possible stream ids depending for protocol v3 and above
 * @const
 * @type {number}
 */
const maxGroupsFor2Bytes = 256;

/**
 * Delay used to check if groups can be released
 * @const
 * @type {number}
 */
// eslint-disable-next-line prefer-const
let releaseDelay = 5000;

/**
 * Represents a queue of ids from 0 to maximum stream id supported by the protocol version.
 * Clients can dequeue a stream id using {@link StreamIdStack#shift()} and enqueue (release) using
 * {@link StreamIdStack#push()}
 */
class StreamIdStack {
  /**
   * Creates a new instance of StreamIdStack.
   * @param {Number} version Protocol version
   * @constructor
   */
  constructor(version) {
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
  setVersion(version) {
    //128 or 32K stream ids depending on the protocol version
    this.maxGroups = types.protocolVersion.uses2BytesStreamIds(version) ? maxGroupsFor2Bytes : 1;
  }

  /**
   * Dequeues an id.
   * Similar to {@link Array#pop()}.
   * @returns {Number} Returns an id or null
   */
  pop() {
    let id = this.currentGroup.pop();
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
  }

  /**
   * Enqueue an id for future use.
   * Similar to {@link Array#push()}.
   * @param {Number} id
   */
  push(id) {
    this.inUse--;
    const groupIndex = id >> shiftToGroup;
    const group = this.groups[groupIndex];
    group.push(id);
    if (groupIndex < this.groupIndex) {
      //Set the lower group to be used to dequeue from
      this.groupIndex = groupIndex;
      this.currentGroup = group;
    }
    this._tryIssueRelease();
  }

  /**
   * Clears all timers
   */
  clear() {
    if (this.releaseTimeout) {
      clearTimeout(this.releaseTimeout);
      this.releaseTimeout = null;
    }
  }

  /**
   * Tries to create an additional group and returns a new id
   * @returns {Number} Returns a new id or null if it's not possible to create a new group
   * @private
   */
  _tryCreateGroup() {
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
  }

  _tryIssueRelease() {
    if (this.releaseTimeout || this.groups.length <= releasableSize) {
      //Nothing to release or a release delay has been issued
      return;
    }
    const self = this;
    this.releaseTimeout = setTimeout(() => self._releaseGroups(), releaseDelay);
  }

  _releaseGroups() {
    let counter = 0;
    let index = this.groups.length - 1;
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
  }
}

function generateGroup(initialValue) {
  const arr = new Array(groupSize);
  const upperBound = initialValue + groupSize - 1;
  for (let i = 0; i < groupSize; i++) {
    arr[i] = upperBound - i;
  }
  return arr;
}

module.exports = StreamIdStack;