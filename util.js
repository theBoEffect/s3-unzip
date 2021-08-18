"use strict";

const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const AdmZip = require("adm-zip");
const fs = require("fs");
const dateTime = require("date-time");
const md5 = require("md5");
const mime = require('mime-types');

//const unzipper = require('unzipper');
//const etl = require('etl');

const { ungzip } = require('node-gzip');

const decompress = async function(/*String*/command, /*Function*/ cb) {
  try {
    console.info('decompress was called');
    if (!command.bucket || !command.file) { //bucket and file are required
      if (cb) return cb(new Error("Error: missing either bucket name or full filename!"));
      console.error("Error: missing either bucket name or full filename!");
      return;
    }

    const filenamePartsArray = command.file.split(".");
    const foldername = filenamePartsArray[0];

    console.info(filenamePartsArray);
    console.info(foldername);
    /**
     * Not sure what this bit was for but keeping it as it is in the original
     
    const d1 = await s3.getObject({ Bucket: command.bucket, Key: foldername+"/" }).promise();
    if (d1) {
      //TODO: if called via command line, ask here to overwrite the data and prompt for response
      //console.log("Folder '"+foldername+"' already exists!");
    }
    */

    console.info('getting data');
    const data = await s3.getObject({ Bucket: command.bucket,Key: command.file }).promise();

    if (command.verbose) console.log(`Zip/Gz file '${command.file}' found in S3 bucket!`);

    // Is this GZ or Zip
    let type = '.zip';
    let mm = 'application/zip';
    if (command.file.toLowerCase().includes('.gz')) {
      console.log('Assuming GZ');
      type = '.gz';
      mm = 'application/gzip';
    } else console.log('Assuming Zip');

    // Write the zip file locally in a tmp dir
    const tmpZipFilename = md5(dateTime({showMilliseconds: true}));
    const fpath = `/tmp/${tmpZipFilename}${type}`;
    fs.writeFileSync(fpath, data.Body);

    // Check that file in that location is a zip content type, otherwise throw error and exit
    if(mime.lookup(fpath) !== mm) {
      fs.unlinkSync(fpath);
      if (cb) return cb(new Error("Error: file is not of type zip or gz. Please select a valid file (filename.zip /.gz)."));
      console.error("Error: file is not of type zip or gz. Please select a valid file (filename.zip / .gz).");
      return;
    }

    // todo, you can just use unzipper for all of it...
    // find all files in the zip and the count of them
    let zipEntries, zipEntryCount;
    if (type === '.zip') {
      console.log('we are using zip');
      const zip = new AdmZip(fpath);
      zipEntries = zip.getEntries();
      zipEntryCount = Object.keys(zipEntries).length;
    } else {
      // using unzipper to get files
      zipEntries = [];
      let temp;
      console.info('We are attempting to decompress GZ');
      console.info(fpath);
      if ( !fs.existsSync('/tmp/gz') ) {
        console.info('making directory');
        fs.mkdirSync('/tmp/gz', { recursive: true });
      }

      console.info('streaming data now');
      const input = fs.readFileSync(fpath);
      console.info('is this of type buffer?');
      console.info(input);
      const output = await ungzip(input)
      console.info('have an output of type buffer?');
      console.info(output);
      console.info('attempting to stream buffer to /tmp/gz');
      fs.createWriteStream('/tmp/gz').write(output);
      /*
      fs.createReadStream(fpath)
      .pipe(unzipper.Parse())
      .pipe(etl.map(entry => {
        console.info('inside parser for unzipper');    
        temp = {};
        temp.entryName = entry.path;
        console.info('filename?');
        console.info(temp.entryName);
        entry.pipe(fs.createWriteStream('/tmp/gz'))
        .on('finish', () => {
          zipEntries.push(temp);
        })
        //const content = await entry.buffer();
        //fs.writeFileSync(`/tmp/gz/${temp.entryName}`, content);
      }));
      */
      console.info('success gz decompress - getting data');
      fs.readdirSync('/tmp/gz').forEach(file => {
        console.info('found: '+file);
        zipEntries.push(file);
      });
      console.info(zipEntries);
      zipEntryCount = zipEntries.length;
      console.info(zipEntryCount);
    }

    console.info('we have the entries.... attempting to write');

    //if no files found in the zip
    if (zipEntryCount === 0){
      console.log('no files found in zip file...');
      fs.unlinkSync(fpath);
      if (cb) return cb(new Error("Error: the zip/gz file was empty!"));
      console.error("Error: the zip/gz file was empty!");
      return;
    }

    //for each file in the zip, decompress and upload it to S3; once all are uploaded, delete the tmp zip and zip on S3
    switch (type) {
      case '.gz':
        // adding this conditional for gz to avoid altering original code too much
        let count = 0;
        for(let i=0;i<zipEntryCount;i++){
          try {
            const data = await s3.upload({Bucket: command.bucket, Key: zipEntries[i].entryName, Body: fs.readFileSync(`/tmp/gz/${zipEntries[i].entryName}`,'utf8')}).promise();
            if (command.verbose) console.log(`File decompressed to S3: ${data.Location}`);
            count = count + 1;
          } catch (err) {
            fs.unlinkSync(fpath);
            if (cb) return cb(new Error(`Upload Error: ${err.message}`));
            console.error(`Upload Error: ${err.message}`);
            return;
          }
        }

        //if all files are not unzipped...
        if (zipEntryCount !== count) {
          if (cb) return cb(new Error(`Counted ${zipEntryCount} but only decompressed ${count}`));
          console.error(`Counted ${zipEntryCount} but only decompressed ${count}`);
          return;
        }

        //delete the tmp (local) zip file
        fs.unlinkSync(fpath);
        if (command.verbose) console.log("Local temp gz file deleted.");
        //delete the zip file up on S3
        if (command.deleteOnSuccess) {
          try {
            const data = await s3.deleteObject({Bucket: command.bucket, Key: command.file}).promise();
            if (command.verbose) console.log("S3 file '"+command.file+"' deleted.");
          } catch (err) {
            if (cb) return cb(new Error(`Delete Error: ${err.message}`));
            console.error(`Delete Error: ${err.message}`);
            return;
          }
        }
        return cb(null, "Success!");

      default:
        console.info('default behavior is to use original zip flow');
        // original s3-unzip code slightly updated, could be updated to async/await later...
        let counter = 0;
        zipEntries.forEach((zipEntry) => {
          s3.upload({ Bucket: command.bucket, Key: zipEntry.entryName, Body: zipEntry.getData() }, function(err, data) {
            counter++;

            if (err) {
              fs.unlinkSync("/tmp/"+tmpZipFilename+".zip");
              if (cb) return cb(new Error("Upload Error: "+err.message));
              console.error("Upload Error: "+err.message);
              return;
            }

            if (command.verbose) console.log("File decompressed to S3: "+data.Location);

            //if all files are unzipped...
            if (zipEntryCount === counter){
              //delete the tmp (local) zip file
              fs.unlinkSync("/tmp/"+tmpZipFilename+".zip");

              if (command.verbose) console.log("Local temp zip file deleted.");

              //delete the zip file up on S3
              if (command.deleteOnSuccess) {
                s3.deleteObject({Bucket: command.bucket, Key: command.file}, function(err, data) {
                  if (err) {
                    if (cb) return cb(new Error("Delete Error: "+err.message));
                    else console.error("Delete Error: "+err.message);
                    return;
                  }

                  if (command.verbose) console.log("S3 file '"+command.file+"' deleted.");

                  //WE GOT TO THE END
                  return cb(null, "Success!");
                });
              }else {
                //WE GOT TO THE END
                return cb(null, "Success!");
              }
            }
          });
        });
    }
  } catch (err) {
    console.error(err);
    if (cb) return cb(new Error("Unexpected Error: "+err.message));
    console.error("Unexpected Error: "+err.message);
    return;
  }
}

module.exports = {
  decompress: decompress
}
