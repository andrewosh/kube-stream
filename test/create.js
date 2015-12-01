var es = require('event-stream') 
var assert = require('assert')

var KubeStream = require('../index.js')
var util = require('./util.js')
var settings = require('../config/main.js')

describe('KubeStream#create()', function () {

  it('should create resources and call the callback with the resource ID', function (done) {
    var stream = new KubeStream(util.baseUrl, 'pods')
    stream.create(util.makePod(1), function (err, pods) {
      if (err) {
        console.log('err: ' + err)
      } else {
        console.log('pods: ' + pods)
      }
      done()
    })
  })

  it('should create resources and return a stream containing the resource ID')

  it('should return a (properly formed) stream if no callback is specified')

  it('should call the callback (and close the stream) if the callback is specified')
})

describe('KubeStream#get()', function () {
  it('should get all pods currently on the cluster', function (done) {
    var stream = new KubeStream(util.baseUrl, 'pods')
    stream.get().pipe(es.through(function write(data) {
      assert(typeof data === 'object')     
      assert.notEqual(data.metadata, null)
      assert.equal(data.status.phase, 'Running')
    }, function end() {
      done()
    })).on('error', function (err) {
      assert.fail(err)
    })
  })

})
