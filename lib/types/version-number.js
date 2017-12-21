'use strict';

const _versionPattern = /(\d+)\.(\d+)(\.\d+)?(\.\d+)?([~-]\w[.\w]*(?:-\w[.\w]*)*)?(\+[.\w]+)?/;

class VersionNumber {
  constructor(major, minor, patch, dsePatch, preReleases, build) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.dsePatch = dsePatch;
    this.preReleases = preReleases;
    this.build = build;
  }

  /**
   * @return {String} String representation of this version.
   */
  toString() {
    let str = this.major + '.' + this.minor;
    if (typeof this.patch !== 'undefined') {
      str += '.' + this.patch;
    }
    if (typeof this.dsePatch !== 'undefined') {
      str += '.' + this.dsePatch;
    }
    if (typeof this.preReleases !== 'undefined') {
      this.preReleases.forEach((preRelease) => {
        str += '-' + preRelease;
      });
    }
    if (this.build) {
      str += '+' + this.build;
    }
    return str;
  }

  /**
   * Compares this version with the provided version. 
   * @param {VersionNumber} other 
   * @return {Number} -1 if less than other, 0 if equal, 1 if greater than.
   */
  compare(other) {
    if (this.major < other.major) {
      return -1;
    } else if (this.major > other.major) {
      return 1;
    } else if (this.minor < other.minor) {
      return -1;
    } else if (this.minor > other.minor) {
      return 1;
    }

    // sanitize patch by setting to 0 if undefined.
    const thisPatch = typeof this.patch !== 'undefined' ? this.patch : 0;
    const otherPatch = typeof other.patch !== 'undefined' ? other.patch : 0;
    if (thisPatch < otherPatch) {
      return -1;
    } else if (thisPatch > otherPatch) {
      return 1;
    }

    // if dsePatch is set in one case, but not other, consider the one where it is set as greater.
    if (typeof this.dsePatch === 'undefined') {
      if (typeof other.dsePatch !== 'undefined') {
        return -1;
      }
    } else if (typeof other.dsePatch === 'undefined') {
      return 1;
    } else {
      if (this.dsePatch < other.dsePatch) {
        return -1;
      } else if (this.dsePatch > other.dsePatch) {
        return 1;
      }
    }

    // If prereleases are present, consider less than those that don't have any.
    if (typeof this.preReleases === 'undefined') {
      if (typeof other.preReleases !== 'undefined') {
        return 1;
      }
    } else if (typeof other.preReleases === 'undefined') {
      return -1;
    }
   
    // Don't build.
    return 0;
  }

  static parse(version) {
    if (!version) {
      return null;
    }

    const match = version.match(_versionPattern);
    if (match) {
      const major = parseInt(match[1], 10);
      const minor = parseInt(match[2], 10);
      const patchStr = match[3];
      const patch = patchStr ? parseInt(patchStr.substring(1), 10) : undefined;
      const dsePatchStr = match[4];
      const dsePatch = dsePatchStr ? parseInt(dsePatchStr.substring(1), 10) : undefined;
      const preReleasesStr = match[5];
      const preReleases = preReleasesStr ? preReleasesStr.substring(1).split('-') : undefined;
      const buildStr = match[6];
      const build = buildStr ? buildStr.substring(1) : undefined;
      return new VersionNumber(major, minor, patch, dsePatch, preReleases, build);
    }
    throw new TypeError('Could not extract version from \'' + version + '\'');
  }
}

module.exports = VersionNumber;