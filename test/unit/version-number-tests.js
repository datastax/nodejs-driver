'use strict';

const assert = require('assert');
const VersionNumber = require('../../lib/types/version-number');

describe('VersionNumber', () => {
  describe('#parse()', () => {
    it('should parse release version', validateVersion('3.0.13', 3, 0, 13));
    it('should parse release without patch', validateVersion('3.11', 3, 11));
    it('should parse pre-release version', validateVersion('4.0.0-beta2-SNAPSHOT', 4, 0, 0, undefined, ['beta2', 'SNAPSHOT']));
    it('should allow tilde as first pre release delimiter', validateVersion('4.0.0~beta2-SNAPSHOT', 4, 0, 0, undefined, ['beta2', 'SNAPSHOT'], undefined, '4.0.0-beta2-SNAPSHOT'));
    it('should parse dse patch', validateVersion('3.0.11.154-SNAPSHOT', 3, 0, 11, 154, ['SNAPSHOT']));
    it('should parse build', validateVersion('4.0.0+build7', 4, 0, 0, undefined, undefined, 'build7'));
    it('should throw TypeError on invalid version', function () {
      assert.throws(() => VersionNumber.parse('notaversion'), TypeError);
    });
  });
  describe('#compare()', () => {
    it('should compare major versions, less than', validateCompare('1.2.0', '2.0.0', -1));
    it('should compare major versions, greater than', validateCompare('2.0.0', '1.2.0', 1));
    it('should compare minor versions, less than', validateCompare('1.1.1', '1.2.1', -1));
    it('should compare minor versions, greater than', validateCompare('1.2.1', '1.1.1', 1));
    it('should compare patch versions, less than', validateCompare('1.2.1', '1.2.2', -1));
    it('should compare patch versions, greater than', validateCompare('1.2.2', '1.2.1', 1));
    it('should compare dse patch versions, less than', validateCompare('1.2.1.1', '1.2.1.2', -1));
    it('should compare dse patch versions, greater than', validateCompare('1.2.1.2', '1.2.1.1', 1));
    it('should compare shortened vs. longer versions, equal', validateCompare('1.2.0', '1.2', 0));
    it('should compare shortened vs. longer versions, less than', validateCompare('1.2', '1.2.1', -1));
    it('should compare shortened vs. longer versions, greater than', validateCompare('1.2.1', '1.2', 1));
    it('should compare dse patch versions, less than', validateCompare('1.2.1.1', '1.2.1.2', -1));
    it('should consider dse patch version as greater, less than', validateCompare('1.2.1', '1.2.1.0', -1));
    it('should consider dse patch version as greater, greater than', validateCompare('1.2.1.0', '1.2.1', 1));
    it('should compare release and prerelease, release greater than', validateCompare('1.2.0', '1.2.0-SNAPSHOT', 1));
    it('should compare release and prerelease, prerelease lesser than', validateCompare('1.2.0-SNAPSHOT', '1.2.0', -1));
    it('should compare prerelease and prerelease, equal', validateCompare('1.2.0-SNAPSHOT', '1.2.0-beta1', 0));
    it('should ignore build number', validateCompare('1.2.0+build1', '1.2.0', 0));
  });
});

function validateVersion(versionStr, major, minor, patch, dsePatch, preReleases, build, toStr) {
  return function() {
    if (!toStr) {
      toStr = versionStr;
    }
    const versionNumber = VersionNumber.parse(versionStr);
    assert.strictEqual(versionNumber.major, major);
    assert.strictEqual(versionNumber.minor, minor);
    assert.strictEqual(versionNumber.patch, patch);
    assert.strictEqual(versionNumber.dsePatch, dsePatch);
    assert.deepEqual(versionNumber.preReleases, preReleases);
    assert.strictEqual(versionNumber.build, build);
    assert.strictEqual(versionNumber.toString(), toStr);
  };
}

function validateCompare(version1, version2, expected) {
  return function () {
    const versionNumber1 = VersionNumber.parse(version1);
    const versionNumber2 = VersionNumber.parse(version2);
    assert.strictEqual(versionNumber1.compare(versionNumber2), expected);
  };
}