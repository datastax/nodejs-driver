/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


const _versionPattern = /(\d+)\.(\d+)(?:\.(\d+))?(?:\.(\d+)?)?(?:[-~]([\w+]*(?:-\w[.\w]*)*))?(?:\+([.\w]+))?/;

/**
 * Represents a version number in the form of X.Y.Z with optional pre-release and build metadata.
 *
 * Version numbers compare the usual way, the major version number (X) is compared first, then
 * the minor one (Y) and then the patch level one (Z).  If pre-release or other build metadata
 * is present for a version, that version is considered less than an otherwise equivalent version
 * that doesn't have these labels, otherwise they are considered equal.
 *
 * As of initial implementation versions are only compared against those with at most patch versions
 * more refined comparisons are not needed.
 *
 * @property {Number} major The major version, X of X.Y.Z.
 * @property {Number} minor The minor version, Y of X.Y.Z.
 * @property {Number} patch The patch version, Z of X.Y.Z.
 * @property {Number} dsePatch The dsePatch version, A of X.Y.Z.A or undefined if not present.
 * @property {String[]} preReleases Prerelease indicators if present, i.e. SNAPSHOT of X.Y.Z-SNAPSHOT.
 * @property {String} build Build string if present, i.e. build1 of X.Y.Z+build1.
 *
 * @ignore @internal
 */
class VersionNumber {
  major: number;
  minor: number;
  patch: number;
  dsePatch: number;
  preReleases: string[];
  build: string;

  /**
   * Represents a version number in the form of X.Y.Z with optional pre-release and build metadata.
   *
   * Version numbers compare the usual way, the major version number (X) is compared first, then
   * the minor one (Y) and then the patch level one (Z).  If pre-release or other build metadata
   * is present for a version, that version is considered less than an otherwise equivalent version
   * that doesn't have these labels, otherwise they are considered equal.
   *
   * As of initial implementation versions are only compared against those with at most patch versions
   * more refined comparisons are not needed.
   *
   * @property {Number} major The major version, X of X.Y.Z.
   * @property {Number} minor The minor version, Y of X.Y.Z.
   * @property {Number} patch The patch version, Z of X.Y.Z.
   * @property {Number} dsePatch The dsePatch version, A of X.Y.Z.A or undefined if not present.
   * @property {String[]} preReleases Prerelease indicators if present, i.e. SNAPSHOT of X.Y.Z-SNAPSHOT.
   * @property {String} build Build string if present, i.e. build1 of X.Y.Z+build1.
   *
   * @ignore @internal
   */
  constructor(major: number, minor: number, patch?: number, dsePatch?: number, preReleases?: string[], build?: string) {
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
  toString(): string {
    let str = this.major + '.' + this.minor;
    if (this.patch !== undefined) {
      str += '.' + this.patch;
    }
    if (this.dsePatch !== undefined) {
      str += '.' + this.dsePatch;
    }
    if (this.preReleases !== undefined) {
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
  compare(other: VersionNumber): number {
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
    const thisPatch = this.patch || 0;
    const otherPatch = other.patch || 0;
    if (thisPatch < otherPatch) {
      return -1;
    } else if (thisPatch > otherPatch) {
      return 1;
    }

    // if dsePatch is set in one case, but not other, consider the one where it is set as greater.
    if (this.dsePatch === undefined) {
      if (other.dsePatch !== undefined) {
        return -1;
      }
    } else if (other.dsePatch === undefined) {
      return 1;
    } else {
      if (this.dsePatch < other.dsePatch) {
        return -1;
      } else if (this.dsePatch > other.dsePatch) {
        return 1;
      }
    }

    // If prereleases are present, consider less than those that don't have any.
    if (this.preReleases === undefined) {
      if (other.preReleases !== undefined) {
        return 1;
      }
    } else if (other.preReleases === undefined) {
      return -1;
    }
   
    // Don't consider build.
    return 0;
  }

  static parse(version: string) {
    if (!version) {
      return null;
    }

    const match = version.match(_versionPattern);
    if (match) {
      const major = parseInt(match[1], 10);
      const minor = parseInt(match[2], 10);
      const patch = match[3] ? parseInt(match[3], 10) : undefined;
      const dsePatch = match[4] ? parseInt(match[4], 10) : undefined;
      const preReleases = match[5] ? match[5].split('-') : undefined;
      const build = match[6];
      return new VersionNumber(major, minor, patch, dsePatch, preReleases, build);
    }
    throw new TypeError('Could not extract version from \'' + version + '\'');
  }
}

export default VersionNumber;