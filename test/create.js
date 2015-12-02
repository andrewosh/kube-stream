var _ = require('lodash')
var es = require('event-stream') 
var assert = require('assert')
var util = require('kube-test-utils')

var KubeClient = require('../index.js')
var settings = require('../config/main.js')

var namespace = 'test'
var client = null

before(function () {
  client = new KubeClient()
})
 
describe('KubeClient#create()', function () {

  it('should correctly create namespaces', function (done) {
    var testNamespace = util.makeNamespace(namespace)
    client.namespaces.create({
      template: testNamespace
    }, function (err, namespace) {
      if (err) throw err
      done()
    })
  })

  it('should not try to create resources twice', function (done) {
    var testNamespace = util.makeNamespace(namespace)
    client.namespaces.create({
      template: testNamespace
    }, function (err, namespace) {
      assert.notEqual(err, null)
      done()
    })
  })

  it('should create pods and call the callback with the created resource', function (done) {
    var template = util.makePod(namespace, 'pod-1')
    client.pods.create({ 
      template: template
    }, function (err, pod) {
      if (err) throw err
      done()
    })
  })

  it('should create services and call the callback with the created resource', function (done) {
    var template = util.makeService(namespace, 'service-1', 'pod-1')
    client.services.create({ 
      template: template
    }, function (err, service) {
      if (err) throw err
      done()
    })
  })
})

describe('KubeClient#get()', function () {

  it('should get all pods currently on the cluster', function (done) {
    client.pods.get(function (err, pods) {
      assert.equal(err, null)
      _.map(pods, function (pod) {
        // simple check of the result structure
        assert.notEqual(pod.metadata, null)
      })
      done()
    })
  })

  it('should get pods in a namespace', function (done) {
    client.pods.get({
      template: util.makePod(namespace, 'pod-1')
    }, function (err, pods) {
      if (err) throw err
      assert.equal(pods.length, 1)
      _.map(pods, function (pod) {
        // simple check of the result structure
        assert.notEqual(pod.metadata, null)
      })
      done()
    })
  })

  it('should get services in a namespace', function (done) {
    client.services.get({
      template: util.makeService(namespace, 'service-1', 'pod-1')
    }, function (err, services) {
      if (err) throw err
      assert.equal(services.length, 1)
      _.map(services, function (service) {
        // simple check of the result structure
        assert.notEqual(service.metadata, null)
      })
      done()
    })
  })

  it('should get pods with a given label')

})

describe('KubeClient#delete()', function () {

  it('should delete services in a namespace', function (done) {
    client.services.delete({
      template: util.makeService(namespace, 'service-1', 'pod-1')
    }, function (err, service) {
      if (err) throw err
      done()
    })
  })

  it('should delete pods in a namespace', function (done) {
    client.pods.delete({
      template: util.makePod(namespace, 'pod-1')
    }, function (err, pod) {
      if (err) throw err
      done()
    })
  })

  it('should result in an empty namespace after both deletions', function (done) {
    var pod = util.makePod(namespace, 'pod-1') 
    client.pods.get({
      template: pod
    }, function (err, pods) {
      if (err) throw err
      assert.equal(pods.length, 0)
      var service = util.makeService(namespace, 'service-1', 'pod-1')
      client.services.get({
        template: service
      }, function (err, services) {
        if (err) throw err
        assert.equal(services.length, 0)
        done()
      })
    })
  })

  it('should delete a namespace', function (done) {
    var ns = util.makeNamespace(namespace)
    client.namespaces.delete({
      template: ns
    }, function (err, ns) {
      if (err) throw err
      done()
    })
  })

})

describe('KubeClient#when()', function () {

  it('should return a namespace once it is active', function (done) {
    this.timeout(5000)
    var ns = util.makeNamespace('test2') 
    client.namespaces.when({
      template: ns,
      condition: function (namespaces) {
        return _.some(_.map(namespaces, function (namespace) {
          return _.isMatch(namespace, { status: { phase: 'Active' }})
        }))
      }
    }, function (err, namespaces) {
      if (err) throw err
      assert.notEqual(namespaces.length, 0)
      done()
    })
    client.namespaces.create({
      template: ns
    }, function (err, namespace) {
      if (err) throw err
    })
  })

  it('should return a pod once it is running', function (done) {
    this.timeout(5000)
    var pod = util.makePod('test2', 'pod2')
    client.pods.when({
      template: pod,
      condition: function (pods) {
        return _.some(_.map(pods, function (pod) {
          return _.isMatch(pod, { status: { phase: 'Running' }})
        }))
      }
    }, function (err, pods) {
      if (err) throw err
      assert.notEqual(pods.length, 0)
      done()
    })
    client.pods.create({
      template: pod
    }, function (err, pod) {
      if (err) throw err
    })
  })

  it('should return a namespace once it has been deleted', function (done) {
    this.timeout(5000)
    var ns = util.makeNamespace('test2') 
    client.namespaces.when({
      template: ns,
      condition: function (namespaces) {
        return (namespaces.length == 0)
      }
    }, function (err, namespaces) {
      if (err) throw err
      assert.equal(namespaces.length, 0)
      done()
    })
    client.namespaces.delete({
      template: ns
    }, function (err, namespace) {
      if (err) throw err
    })
  })

})

describe('KubeClient#watch()', function () {

  var events = require('../lib/events.js')

  it('should generate the correct events when resources are created', function (done) {
    this.skip()
    var namespace = util.makeNamespace('test3')
    var nsStream = client.namespaces.watch({
      template: namespace
    })
      .pipe(events.hasState({ object: { status: { phase: 'Active' }}}))
    nsStream.on('data', function (data) {
      console.log('data: ' + JSON.stringify(data))
      this.destroy()
      return done()
    })
    nsStream.on('error', function (err) {
      throw err
    })
    nsStream.on('end', function () {
      assert.fail('Stream ended before correct state event was emitted')
    })
    setTimeout(function () { 
      client.namespaces.create({
        template: namespace
      }, function (err, ns) {
        if (err) throw err
      })
    }, 1000)
  })

  it('should generate the correct events when resources are deleted', function (done) {
    this.skip()
    var namespace = util.makeNamespace('test3')
    var nsStream = client.namespaces.watch({
      template: namespace
    }).pipe(events.hasState({ object: { status: { phase: 'Terminating' }}}))
    nsStream.on('data', function (data) {
      console.log('data: ' + JSON.stringify(data))
      this.destroy()
      return done()
    })
    nsStream.on('error', function (err) {
      throw err
    })
    nsStream.on('end', function () {
      assert.fail('Stream ended before correct state event was emitted')
    })
    setTimeout(function () {
      client.namespaces.delete({
        template: namespace
      }, function (err, ns) {
        if (err) throw err
      })
    }, 1000)
  })

})
