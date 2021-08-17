"use strict";

var AWS = require("aws-sdk");
var s3 = new AWS.S3();
var AdmZip = require("adm-zip");
var fs = require("fs");
var dateTime = require("date-time");
var md5 = require("md5");
var mime = require('mime-types');
var zlib = require('zlib');

//const dcomp = require('decompress');
//const dcompGz = require('decompress-gz');

// checks whether a file exists
function fileExists(filePath) {
  try {
      return fs.statSync(filePath).isFile();
  } catch (err) {
      return false;
  }
}

function gunzip(source, destination, callback) {
  try {
    // check if source file exists
    if ( !fileExists(source) ) {
      throw new Error(`file not found: ${source}`);
    }
    // prepare streams
    var src = fs.createReadStream(source);
    var dest = fs.createWriteStream(destination);

    // extract the archive
    src.pipe(zlib.createGunzip()).pipe(dest);

    // callback on extract completion
    dest.on('close', function() {
      if ( typeof callback === 'function' ) {
        callback();
      }
    });
  } catch (err) {
    // either source is not readable
    // or the destination is not writable
    // or file not a gzip
    callback(err);
  }
}

function gzAsync (path, dest) {
  return new Promise((resolve, reject) => {
    try {
      gunzip(path, dest, function(err) {
        if(err) {
          return reject(err);
        }
        const items = [];
        fs.readdirSync(dest).forEach(file => {
          items.push(file);
        });
        return resolve(items);
      });
    } catch (error) {
      return reject(error)
    }
  });
};

var decompress = function(/*String*/command, /*Function*/ cb) {

  if (!command.bucket || !command.file) { //bucket and file are required
    if (cb) cb(new Error("Error: missing either bucket name or full filename!"));
    else console.error("Error: missing either bucket name or full filename!");
    return;
  }

  var filenamePartsArray = command.file.split(".");
  var foldername = filenamePartsArray[0];

  s3.getObject(
    {
      Bucket: command.bucket,
      Key: foldername+"/"
    }, async function(err, data) {
      if (data) {
        //TODO: if called via command line, ask here to overwrite the data and prompt for response
        //console.log("Folder '"+foldername+"' already exists!");
      }

      s3.getObject(
        {
          Bucket: command.bucket,
          Key: command.file
        }, async function(err, data) {
          try {
            if (err) {
              if (cb) cb(new Error("File Error: "+err.message));
             else console.error("File Error: "+err.message);
             return;
           } else {
              if (command.verbose) console.log("Zip/Gz file '"+command.file+"' found in S3 bucket!");
             let type = '.zip';
             let mm = 'application/zip';
             if (command.file.toLowerCase().includes('.gz')) {
               console.log('Assuming GZ');
               type = '.gz';
               mm = 'application/gzip';
             } else console.log('Assuming Zip');
 
             //write the zip file locally in a tmp dir
             var tmpZipFilename = md5(dateTime({showMilliseconds: true}));
             const fpath = `/tmp/${tmpZipFilename}${type}`;
             fs.writeFileSync(fpath, data.Body);
 
             //check that file in that location is a zip content type, otherwise throw error and exit
             if(mime.lookup(fpath) !== mm) {
               if (cb) cb(new Error("Error: file is not of type zip or gz. Please select a valid file (filename.zip /.gz)."));
               else console.error("Error: file is not of type zip or gz. Please select a valid file (filename.zip / .gz).");
               fs.unlinkSync(fpath);
               return;
             }
 
 
             //find all files in the zip and the count of them
             let zipEntries, zipEntryCount;
             if (type === '.zip') {
               const zip = new AdmZip(fpath);
               zipEntries = zip.getEntries();
               zipEntryCount = Object.keys(zipEntries).length;
             } else {
               // using decompress to get files in gz
               try {
                  console.info('We are attempting to decompress GZ');
                  console.info(fpath);
                  zipEntries = await gzAsync(fpath, '/tmp/gz');
                  console.info('success gz decompress');
                  console.info(zipEntries);
                  zipEntryCount = zipEntries.length;
               } catch (err) {
                  console.error(err);
                  if (cb) cb(new Error("Unexpected Error: "+err.message));
                  else console.error("Unexpected Error: "+err.message);
                  return;
               }
             }
 
 
             //if no files found in the zip
             if (zipEntryCount === 0){
               if (cb) cb(new Error("Error: the zip/gz file was empty!"));
               else console.error("Error: the zip/gz file was empty!");
               fs.unlinkSync(fpath);
               return;
             }
 
             //for each file in the zip, decompress and upload it to S3; once all are uploaded, delete the tmp zip and zip on S3
             if (type === '.gz') {
               // adding this conditional for gz to avoid altering original code too much
               let count = 0;
               for(let i=0;i<zipEntryCount;i++){
                  try {
                    const data = await s3.upload({Bucket: command.bucket, Key: zipEntries[i].path, Body: fs.readFileSync(zipEntries[i].path,'utf8')}).promise();
                    if (command.verbose) console.log("File decompressed to S3: "+data.Location);
                    count = count + 1;
                  } catch (err) {
                    if (cb) cb(new Error("Upload Error: "+err.message));
                    else console.error("Upload Error: "+err.message);
                    fs.unlinkSync(fpath);
                    return;
                  }
               }
 
               //if all files are unzipped...
               if (zipEntryCount === count){
                 //delete the tmp (local) zip file
                 fs.unlinkSync(fpath);
 
                 if (command.verbose) console.log("Local temp gz file deleted.");
 
                 //delete the zip file up on S3
                 if (command.deleteOnSuccess) {
                   s3.deleteObject({Bucket: command.bucket, Key: command.file}, function(err, data) {
                     if (err) {
                       if (cb) cb(new Error("Delete Error: "+err.message));
                       else console.error("Delete Error: "+err.message);
                       return;
                     }
 
                     if (command.verbose) console.log("S3 file '"+command.file+"' deleted.");
 
                     //WE GOT TO THE END
                     cb(null, "Success!");
                   });
                 }else {
                   //WE GOT TO THE END
                   cb(null, "Success!");
                 }
               } else {
                 if (cb) cb(new Error(`Counted ${zipEntryCount} but only decompressed ${count}`));
                 else console.error(`Counted ${zipEntryCount} but only decompressed ${count}`);
                 return;
               }
             } else {
               // original zip code
               var counter = 0;
               zipEntries.forEach(function(zipEntry) {
                 s3.upload({ Bucket: command.bucket, Key: zipEntry.entryName, Body: zipEntry.getData() }, function(err, data) {
                   counter++;
   
                   if (err) {
                     if (cb) cb(new Error("Upload Error: "+err.message));
                     else console.error("Upload Error: "+err.message);
                     fs.unlinkSync("/tmp/"+tmpZipFilename+".zip");
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
                           if (cb) cb(new Error("Delete Error: "+err.message));
                           else console.error("Delete Error: "+err.message);
                           return;
                         }
   
                         if (command.verbose) console.log("S3 file '"+command.file+"' deleted.");
   
                         //WE GOT TO THE END
                         cb(null, "Success!");
                       });
                     }else {
                       //WE GOT TO THE END
                       cb(null, "Success!");
                     }
                   }
                 });
               });
             }
           }
          } catch (err) {
            console.error(err);
            if (cb) cb(new Error("Unexpected Error: "+err.message));
            else console.error("Unexpected Error: "+err.message);
            return;
          }
        }
      );
    }
  );
}

module.exports = {
  decompress: decompress
}
