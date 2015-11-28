'use strict'

var duplexify = require('duplexify')
var through = require('through2')
var mergeStream = require('merge-stream')
var MultiFork = require('multi-fork')
var _ = require('underscore')

function Partition(key, ranges, outputs) {

  if(ranges.length > outputs.length) {
    throw new Error('Add more outputs')
  }

  var multiStream = new MultiFork(outputs.length, {
    classifier: function (doc, cb) {
      var index = _.indexOf(ranges, doc[key])
      /**
       * The default output for values out of range is at the last index.
       * _.indexOf will return -1 if the value is out of range.
       */
      if( index < 0 ) {
        index = _.findLastIndex(outputs)
      }
      return cb(null, index)
    }
  })

  var mergedStream = mergeStream()
      mergedStream.add(multiStream.streams)

  for (var index in multiStream.streams) {
    // redirect outputs Stream errors to mergedStream
    outputs[index].on('error', function(err) {
      mergedStream.emit('error', err)
    })

    // pipe out streams to their outputs
    multiStream.streams[index].pipe(outputs[index])
  }

  // Side output with all streams merged
  var sideStream = through.obj(function (chunk, enc, callback) {
    callback()
  })

  // send everything side-stream
  mergedStream.pipe(sideStream)

  // redirect mergedStream errors to sideStream
  mergedStream.on('error', function(err) { sideStream.emit('error', err) })

  return duplexify.obj(multiStream, sideStream);
}

module.exports = Partition
