#!/usr/bin/env node

var partition = require("./")
var _ = require('highland')

/*
  Let's say we have a stream of objects. Each object has a 'type' property,
  and we want to send each object to a different stream according to its type.
  */

var docs = [
  {id:0, type: 'apple'},
  {id:1, type: 'apple'},
  {id:2, type: 'banana'},
  {id:3, type: 'coco'},
  {id:4, type: 'coco'},
  {id:5, type: 'potato'},
  {id:6, type: 'tomato'}
]

/*
  This is partition key; the objects will be partitioned by 'type' property.
 */

var key = 'type'

/*
  These are partition ranges; each range will be assigned to a different stream.
 */

var ranges = ['apple', 'banana', 'coco']

/*
  Now, let's create four bogus streams; that will be our receivers.
 */

var streamJohn = _().each(function(doc) { console.log(doc.id, doc.type, 'sent to John') })
var streamAnna = _().each(function(doc) { console.log(doc.id, doc.type, 'sent to Anna') })
var streamBill = _().each(function(doc) { console.log(doc.id, doc.type, 'sent to Bill') })
var outOfRange = _().each(function(doc) { console.log(doc.id, doc.type, 'is not a fruit') })

/*
  The last 'outOfRange' stream is special. It will receive all the objects,
  that do not match to any of the ranges that we have defined.
  But first, put all receivers together into an array of streams.
 */

var outputStreams = [
  streamJohn, // respectively, John should get all the Apples
  streamAnna, // Anna will get Banana
  streamBill, // and Bill two Coconuts
  outOfRange  // All non-fruits (out of defined ranges) will go here
  // Important: The outOfRange stream has to be at last position in array
]

_(docs).pipe(partition(key, ranges, outputStreams))
