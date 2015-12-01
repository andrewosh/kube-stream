var es = require('event-stream') 
var assert = require('assert')

var KubeStream = require('../index.js')
var util = require('./util.js')
var settings = require('../config/main.js')

describe('KubeStream#create()', function () {

  it('should create resources and call the callback with the resource ID', function (done) {
    this.timeout(20000)
    var stream = new KubeStream({ name: 'pods'})
    var template = util.makePod(1)
    stream.create(util.makePod(1), function (err) {
      var getStream = stream.get({ template: template })
      getStream.on('error', function (err) {
        assert.fail(err)
      })
      getStream.pipe(es.through(function write(pod) {
        console.log(pod)
        assert.notEqual(pod, null)
        assert.equal(_.get(pod, 'metadata.name'), template.metadata.name)
        var status = _.get(pod, 'status.phase')
        assert((status === 'Running') || (status === 'Pending'))
      }, function end() {
        done()
      }))
    })
  })

  it('should create resources and return a stream containing the resource ID', function (done) {
  })

  it('should return a (properly formed) stream if no callback is specified')

  it('should call the callback (and close the stream) if the callback is specified')
})

describe('KubeStream#get()', function () {
  it('should get all pods currently on the cluster', function (done) {
    var stream = new KubeStream({ name: 'pods'})
    stream.get().pipe(es.through(function write(data) {
      assert(typeof data === 'object')     
      // simple checks to ensure these are likely properly formed
      assert(data.metadata)
      assert(data.status.phase)
    }, function end() {
      done()
    })).on('error', function (err) {
      assert.fail(err)
    })
  })
})

describe('KubeStream#watch()', function () {
  it('should watch for updates as they are generated', function (done) {
    this.timeout(20000)
    var stream = new KubeStream({ name: 'pods' })
    stream.watch().pipe(es.through(function write(data) {
      // Make sure creation/deletion is detected here, then done()
      // console.log('data: ' + data)
    }, function end() {
      done()
    }))
  })
})
