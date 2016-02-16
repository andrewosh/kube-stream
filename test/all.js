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
 
describe('KubeClient', function () {

  describe('create', function () {

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

  describe('get', function () {

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

  describe('delete', function () {

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

  describe('patch', function () {

    var client = null

    before(function (done) {
      this.timeout(10000)
      var ns = util.makeNamespace('patch')
      client = new KubeClient()
      client.namespaces.changeState({
        state: ns,
        delta: { status: { phase: 'Active' }},
        action: client.namespaces.create
      }, function (err, namespace) {
        if (err) throw err
        done()
      })
    })

    it('should patch an existing resource', function (done) {
      this.timeout(10000)
      var pod = util.makePod('patch', 'pod1')
      client.pods.changeState({
        state: pod,
        delta: { status: { phase: 'Running' } },
        action: client.pods.create
      }, function (err, pod) {
        if (err) throw err
        client.pods.patch({
          template: pod,
          type: 'add',
          patch: { metadata: { labels: { hello: 'world' } } }
        }, function (err, patched) {
          if (err) throw err
          console.log('patched: ' + JSON.stringify(patched))
          var labels = _.get(patched, 'metadata.labels')
          assert(labels.hello === 'world')
          done()
        })
      })
    })

    it('should fail to patch a nonexistent resource')

    after(function (done) {
      this.skip()
      var ns = util.makeNamespace('patch')
      client.namespaces.delete({
        template: ns
      }, function (err, ns) {
        if (err) throw err
        done()
      })
    })

  })

  describe('when', function () {

    it('should return a namespace once it is active', function (done) {
      this.timeout(5000)
      var ns = util.makeNamespace('when') 
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
      var pod = util.makePod('when', 'pod2')
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
      var ns = util.makeNamespace('when') 
      client.namespaces.when({
        template: ns,
        condition: function (namespaces) {
          return (namespaces.length == 0)
        }
      }, function (err, isEmpty) {
        if (err) throw err
        assert(isEmpty)
        done()
      })
      client.namespaces.delete({
        template: ns
      }, function (err, namespace) {
        if (err) throw err
      })
    })

  })

  describe('changeState', function () {

    it('should correctly create and wait for a new namespace', function (done) {
      this.timeout(10000)
      var ns = util.makeNamespace('changestate')
      var client = new KubeClient()
      client.namespaces.changeState({
        state: ns,
        delta: { status: { phase: 'Active' }},
        action: client.namespaces.create
      }, function (err, namespace) {
        if (err) throw err
        done()
      })
    })

    it('should correctly create and wait for a new pod', function (done) {
      this.timeout(10000)
      var pod = util.makePod('changestate', 'pod1')
      var client = new KubeClient()
      client.pods.changeState({
        state: pod,
        delta: { status: { phase: 'Running' }},
        action: client.pods.create
      }, function (err, pod) {
        if (err) throw err
        done()
      })
    })

    it('should correctly wait for the deletion of a namespace', function (done) {
      this.timeout(10000) 
      var ns = util.makeNamespace('changestate')
      var client = new KubeClient()
      client.namespaces.changeState({
        state: ns,
        condition: function (namespaces) {
          return !_.find(namespaces, { metadata: { name: _.get(ns, 'metadata.name') }})
        },
        action: client.namespaces.delete
      }, function (err, namespace) {
        if (err) throw err
        done()
      })
    })

  })

  describe('watch', function () {

    var events = require('../lib/events.js')

    it('should generate the correct events when resources are created', function (done) {
      this.skip()
      var namespace = util.makeNamespace('watch')
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
      var namespace = util.makeNamespace('watch')
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
})
