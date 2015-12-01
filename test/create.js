var _ = require('lodash')
var es = require('event-stream') 
var assert = require('assert')

var KubeClient = require('../index.js')
var util = require('./util.js')
var settings = require('../config/main.js')

var namespace = 'test'

describe('KubeClient#create()', function () {

  it('should correctly create namespaces', function (done) {
    var client = new KubeClient({ name: 'namespaces'})
    var testNamespace = util.makeNamespace(namespace)
    client.create({
      template: testNamespace
    }, function (err, namespace) {
      if (err) throw err
      done()
    })
  })

  it('should not try to create resources twice', function (done) {
    var client = new KubeClient({ name: 'namespaces'})
    var testNamespace = util.makeNamespace(namespace)
    client.create({
      template: testNamespace
    }, function (err, namespace) {
      assert.notEqual(err, null)
      done()
    })
  })

  it('should create pods and call the callback with the created resource', function (done) {
    var client = new KubeClient({ name: 'pods'})
    var template = util.makePod(namespace, 'pod-1')
    client.create({ 
      template: template
    }, function (err, pod) {
      if (err) throw err
      console.log('pod: ' + pod)
      done()
    })
  })

  it('should create services and call the callback with the created resource', function (done) {
    var client = new KubeClient({ name: 'services'})
    var template = util.makeService(namespace, 'service-1', 'pod-1')
    client.create({ 
      template: template
    }, function (err, service) {
      if (err) throw err
      console.log('service: ' + service)
      done()
    })
  })
})

describe('KubeClient#get()', function () {

  it('should get all pods currently on the cluster', function (done) {
    var client = new KubeClient({ name: 'pods'})
    client.get(function (err, pods) {
      assert.equal(err, null)
      _.map(pods, function (pod) {
        // simple check of the result structure
        assert.notEqual(pod.metadata, null)
      })
      done()
    })
  })

  it('should get pods in a namespace', function (done) {
    var client = new KubeClient({ name: 'pods'})
    client.get({
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
    var client = new KubeClient({ name: 'services'})
    client.get({
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
    var client = new KubeClient({ name: 'services' })
    client.delete({
      template: util.makeService(namespace, 'service-1', 'pod-1')
    }, function (err, service) {
      if (err) throw err
      done()
    })
  })

  it('should delete pods in a namespace', function (done) {
    var client = new KubeClient({ name: 'pods' })
    client.delete({
      template: util.makePod(namespace, 'pod-1')
    }, function (err, pod) {
      if (err) throw err
      done()
    })
  })

  it('should result in an empty namespace after both deletions', function (done) {
    var client = new KubeClient({ name: 'pods' })
    var pod = util.makePod(namespace, 'pod-1') 
    client.get({
      template: pod
    }, function (err, pods) {
      if (err) throw err
      assert.equal(pods.length, 0)
      var client2 = new KubeClient({ name: 'services'})
      var service = util.makeService(namespace, 'service-1', 'pod-1')
      client2.get({
        template: service
      }, function (err, services) {
        if (err) throw err
        assert.equal(services.length, 0)
        done()
      })
    })
  })

  it('should delete a namespace', function (done) {
    var client = new KubeClient({ name: 'namespaces' })
    var ns = util.makeNamespace(namespace)
    client.delete({
      template: ns
    }, function (err, ns) {
      if (err) throw err
      done()
    })
  })

})

describe('KubeClient#watch()', function () {

  it('should watch for updates as they are generated')

})
