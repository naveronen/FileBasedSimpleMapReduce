"use strict";
const fs = require("fs");
const fsPromises = fs.promises;

const path = require("path");
const uuid = require("uuid");
const Promise = require("bluebird");
const events = require("events");
/*

	usage:
		declare the module - 
			base directory is where to generate the temp files
			max records in memory is how much to buffer before dumping to file
			stillAlive - is an optional callback, if you want to know the process isn't stuck
		
		call await mapValue (key,value)
		multiple times until everything is mapped
		
		call await writeDone - to flush everything to files
		

		call await reduceValues with a callback (key,values)		
		
		that's it
		
		if no exceptions happend, module does self cleanup of the temp file, otherwise, it's dirty



*/
module.exports = function (baseDirectory, maxInMemoryRecords, stillAlive) {
  var that = new events.EventEmitter();
  var recordsPerKey = {};
  var allFiles = [];
  var currentRecords = 0;
  var isDoneWrite = false;

  that.writeDone = async () => {
    isDoneWrite = true;

    if (currentRecords > 0) {
      await saveDataToFile({ filename: getNewFileName(), records: { ...recordsPerKey }, stillAlive });
      currentRecords = 0;
      recordsPerKey = {};
    }
  };

  that.mapValue = (key, value) => {
    if (isDoneWrite) {
      throw new Error("mapValue closed after writing is done");
    }

    return new Promise(async (resolve, reject) => {
      currentRecords++;
      if (!recordsPerKey[key]) {
        recordsPerKey[key] = [value];
      } else {
        recordsPerKey[key].push(value);
      }

      if (currentRecords >= maxInMemoryRecords) {
        if (stillAlive) {
          try {
            stillAlive();
          } catch (e) {}
        }

        try {
          await saveDataToFile({ filename: getNewFileName(), records: { ...recordsPerKey }, stillAlive });
        } catch (e) {
          reject(e);
          return;
        }

        if (stillAlive) {
          try {
            stillAlive();
          } catch (e) {}
        }

        currentRecords = 0;
        recordsPerKey = {};
      }

      resolve();
    });
  };

  const filesMerge = async (FilesList, stillAlive) => {
    var newfileName = FilesList[0] + ".merge";
    const writeHandle = await fsPromises.open(newfileName, "wx");
    const fileHandles = await Promise.all(FilesList.map((fname) => fsPromises.open(fname, "r")));
    const readers = fileHandles.map((fh) => {
      var rdr = handleReader(fh);
      var ret = { rdr, line: undefined };

      var nextLine = () => {
        return new Promise(async (res, rej) => {
          var line = await rdr.ReadLine();
          if (line === undefined) {
            ret.line = line;
            res();
          } else {
            try {
              ret.line = [line[0], JSON.parse(line[1])];
              res();
            } catch (ex) {
              rej(ex);
            }
          }
        });
      };

      ret.nextLine = nextLine;

      return ret;
    });

    // get an initial line from all the readers
    await Promise.all(readers.filter((r) => r.line === undefined).map((r) => r.nextLine()));

    const hasMoreToRead = () => {
      return readers.filter((r) => r.line !== undefined).length > 0;
    };

    while (hasMoreToRead()) {
      // find min key
      var minKey = undefined;
      readers.forEach((r) => {
        if (r.line !== undefined) {
          if (minKey === undefined || minKey > r.line[0]) {
            minKey = r.line[0];
          }
        }
      });

      var valuesForMinKey = [];
      var nextLinePromises = [];
      if (stillAlive) {
        try {
          stillAlive();
        } catch (e) {}
      }
      readers.forEach((r) => {
        if (r.line !== undefined && r.line[0] === minKey) {
          valuesForMinKey = valuesForMinKey.concat(r.line[1]);
          nextLinePromises.push(r.nextLine());
        }
      });

      await Promise.all(nextLinePromises);
      var jsonValues = JSON.stringify(valuesForMinKey);
      var fullLine = minKey.length + "|" + minKey + "|" + jsonValues;
      await writeHandle.write(Buffer.from(fullLine).length + "|" + fullLine);
    }

    await writeHandle.close();

    // delete files
    await Promise.all(FilesList.map((fname) => fsPromises.unlink(fname)));

    return newfileName;
  };

  that.reduceValues = async (cbKeyValuesInArray) => {
    return new Promise(async (resolve, reject) => {
      if (!isDoneWrite) {
        reject("reduceValues called before writing is done");
        return;
      }

      // if too many files, we won't be able to open them properly
      // so need to do pre-file merge
      while (allFiles.length > 200) {
        var newfiles = [];
        await asyncForEach(breakArrayToParts(allFiles, 100), async (fileGroup) => {
          if (stillAlive) {
            try {
              stillAlive();
            } catch (e) {}
          }
          var newFN = await filesMerge(fileGroup, stillAlive);
          newfiles.push(newFN);
        });

        allFiles = newfiles;
      }

      // get a handle on all the files
      try {
        const fileHandles = await Promise.all(allFiles.map((fname) => fsPromises.open(fname, "r")));

        // get a pointer array of same length as the handles
        const readers = fileHandles.map((fh) => {
          var rdr = handleReader(fh);
          var ret = { rdr, line: undefined };
          var nextLine = () => {
            return new Promise(async (res, rej) => {
              var line = await rdr.ReadLine();
              if (line === undefined) {
                ret.line = line;
                res();
              } else {
                try {
                  ret.line = [line[0], JSON.parse(line[1])];
                  res();
                } catch (ex) {
                  rej(ex);
                }
              }
            });
          };

          ret.nextLine = nextLine;

          return ret;
        });

        // get an initial line from all the readers
        await Promise.all(readers.filter((r) => r.line === undefined).map((r) => r.nextLine()));

        const hasMoreToRead = () => {
          return readers.filter((r) => r.line !== undefined).length > 0;
        };

        while (hasMoreToRead()) {
          // find min key
          var minKey = undefined;
          readers.forEach((r) => {
            if (r.line !== undefined) {
              if (minKey === undefined || minKey > r.line[0]) {
                minKey = r.line[0];
              }
            }
          });

          var valuesForMinKey = [];
          var nextLinePromises = [];
          readers.forEach((r) => {
            if (r.line !== undefined && r.line[0] === minKey) {
              valuesForMinKey = valuesForMinKey.concat(r.line[1]);
              nextLinePromises.push(r.nextLine());
            }
          });

          await Promise.all(nextLinePromises);
          if (stillAlive) {
            try {
              stillAlive();
            } catch (e) {}
          }

          await cbKeyValuesInArray(minKey, valuesForMinKey);
        }

        // delete files
        await Promise.all(allFiles.map((fname) => fsPromises.unlink(fname)));
        resolve();
      } catch (e) {
        reject(e);
      }
    });
  };

  const getNewFileName = () => {
    // setup a new file name
    return path.join(baseDirectory, uuid());
  };

  const saveDataToFile = ({ filename, records, stillAlive }) => {
    return new Promise(async (resolve, reject) => {
      try {
        if (records.length === 0) {
          resolve();
          return;
        }

        var handle = await fsPromises.open(filename, "wx").catch((ex) => {
          reject(ex);
          return;
        });

        allFiles.push(filename);
        var allkeys = Object.keys(records);

        // this is the critical point - records are saved to the file SORTED!!
        allkeys.sort();

        var outputbuf = "";
        for (var k = 0; k < allkeys.length; k++) {
          var recordsForKey = recordsPerKey[allkeys[k]];

          var jsonValues = JSON.stringify(recordsForKey);
          var fullLine = allkeys[k].length + "|" + allkeys[k] + "|" + jsonValues;

          outputbuf += Buffer.from(fullLine).length + "|" + fullLine;

          if (outputbuf.length > 16000) {
            await handle.write(outputbuf);
            outputbuf = "";
            if (stillAlive) {
              try {
                stillAlive();
              } catch (e) {}
            }
          }
        }

        if (outputbuf.length > 0) {
          await handle.write(outputbuf);
        }

        await handle.close();

        resolve();
      } catch (e) {
        reject(e);
      }
    });
  };

  const handleReader = (handle) => {
    const bufSize = 1024 * 16;
    var buffer = Buffer.alloc(bufSize);
    var filePosition = 0;
    var fileSize = -1;
    var handleClosed = false;

    // wrap content in a function so we get closure
    const ReadLine = () => {
      return new Promise(async (resolve, reject) => {
        // negative file position means EOF
        if (filePosition < 0) {
          if (!handleClosed) {
            handleClosed = true;
            await handle.close(handle);
          }

          resolve(undefined);
          return;
        }

        if (fileSize < 0) {
          var stats = await handle.stat();
          fileSize = stats.size;
        }

        // read some data from the file
        let bytesRead;
        try {
          bytesRead = (await handle.read(buffer, 0, bufSize, filePosition)).bytesRead;
          if (bytesRead === 0) {
            if (!handleClosed) {
              handleClosed = true;
              await handle.close(handle);
            }

            // we can't read no more, so it's EOF
            filePosition = -1;
            resolve(undefined);
            return;
          }
        } catch (e) {
          if (!handleClosed) {
            handleClosed = true;
            await handle.close(handle);
          }

          reject(e);
          return;
        }

        // get the full line length
        var bufferString = buffer.toString();
        var firstSeperator = bufferString.indexOf("|");
        var fullLineLength = parseInt(bufferString.substring(0, firstSeperator));

        var useBigBuffer = false;
        var bigBuffer;
        if (bufferString.length < fullLineLength + 1 + firstSeperator) {
          useBigBuffer = true;
          // need to read more
          bigBuffer = Buffer.alloc(fullLineLength + 1 + firstSeperator);
          bytesRead = (await handle.read(bigBuffer, 0, fullLineLength + 1 + firstSeperator, filePosition)).bytesRead;
        }

        // get the key length
        var secondSeperator = bufferString.indexOf("|", firstSeperator + 1);
        var keyLength = parseInt(bufferString.substring(firstSeperator + 1, secondSeperator));
        var key = bufferString.substring(secondSeperator + 1, secondSeperator + keyLength + 1);
        var body = useBigBuffer
          ? bigBuffer.subarray(secondSeperator + keyLength + 2, fullLineLength + firstSeperator + 1).toString()
          : buffer.subarray(secondSeperator + keyLength + 2, fullLineLength + firstSeperator + 1).toString();
        
        try {
          JSON.parse(body);
        } catch (e) {
        }
        // +1 to account for the 13 because lines ends with 10
        filePosition += fullLineLength + firstSeperator + 1;
        resolve([key, body]);
        return;
      });
    };

    return {
      ReadLine: () => {
        return ReadLine();
      },
    };
  };
  return that;
};

const breakArrayToParts = (arr, size) => {
  var ret = [];
  for (var i = 0; i < arr.length; i += size) {
    ret.push(arr.slice(i, i + size));
  }
  return ret;
};

const asyncForEach = (array, callback) => {
  return new Promise(async (resolve, reject) => {
    for (let index = 0; index < array.length; index++) {
      try {
        await callback(array[index], index, array);
      } catch (e) {
        console.log(e);
        reject(e);
      }
    }
    resolve();
  });
};
