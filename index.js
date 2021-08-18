"use strict";

const Utils = require("./util");

const s3Unzip = {
  async unzip (command, cb) {
    if (cb === undefined) {cb = function(err, success) {};}
    var vBucket, vFile;
    if (command.args && command.args.length >= 2) {
      vBucket = command.args[0];
      vFile = command.args[1];
    }
    if (command.bucket) {
      vBucket = command.bucket;
    }
    if (command.file) {
      vFile = command.file;
    }
    await Utils.decompress({
      bucket: vBucket,
      file: vFile,
      deleteOnSuccess: command.deleteOnSuccess,
      verbose: command.verbose
    }, cb);
  },

  async handler (event, context, callback) {
    if (callback === undefined) {callback = (err, success) => {};}
    await Utils.decompress({
      bucket: event.Records[0].s3.bucket.name,
      file: event.Records[0].s3.object.key,
      deleteOnSuccess: true,
      verbose: true
    }, callback);
  }
}

module.exports = s3Unzip;

