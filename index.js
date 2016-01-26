var _ = require('lodash')
var async = require('async')
var request = require('request')
var es = require('event-stream')
var concat = require('concat-stream')
var urljoin = require('url-join')
var JSONStream = require('JSONStream')
var format = require('string-format')
format.extend(String.prototype)

var util = require('./lib/util.js')
var settings = require('./config/main.js')

/**
 * Manages all communication with a Kubernetes resource endpoint (i.e. 'pods')
 * @constructor
 */
function ResourceClient (opts) {
  opts = opts || {}
  this.name = opts.name 
  this.token = opts.token
  this.baseUrl = opts.baseUrl
}

ResourceClient.prototype._query = function (url, opts) {
  opts = opts || {}
  var query = null
  if (opts.labels) {
    query = '?labelSelector='
    _.forEach(_.keys(opts.labels), function (name) {
      query += '{0}%3D{1}%2'.format(name, opts.labels[name])
    })
  }
  if (query) {
    return url + query
  }
  return url
}

ResourceClient.prototype._requestOpts = function () {
  return {
    headers: {
      'Authorization': 'Bearer {0}'.format(this.token)
    }
  }
}

ResourceClient.prototype._extract = function (items, view) {
  if (!view) {
    return items
  } 
  return _.pluck(items, view)
}

ResourceClient.prototype._processResponse = function (opts, rsp) {
  opts = opts || {}
  var self = this
  // TODO use pump here?
  return rsp
    .pipe(es.split())
    .pipe(JSONStream.parse())
    .pipe(es.through(function write(data) {
      var thr = this
      var items = _.get(data, 'items')
      if (items) { 
        _.forEach(_.get(data, 'items'), function (item) {
          thr.emit('data', item)
        })
      } else {
        thr.emit('data', data)
      }
    }))
    .pipe(es.map(function (data, cb) {
      if (opts.template) {
        var toMatch = data['type'] ? data['object'] : data
        // kind might not consistently appear in the results
        toMatch = _.omit(toMatch, 'kind')
        var tempNoKind = _.omit(opts.template, 'kind')
        if (_.isMatch(toMatch, tempNoKind)) { 
          return cb(null, data)
        }
        return cb()
      }
      return cb(null, data)
    }))
    .pipe(es.map(function (data, cb) {
      cb(null, self._extract(data, opts.view))
    }))
    .pipe(es.through(function write(data) {
      if (data && data['kind'] === 'Status' && data['status'] === 'Failure') {
        this.emit('error', data)
      } else {
        this.emit('data', data)
      }
    }))
}

ResourceClient.prototype.get = function (opts, cb) {
  if (!this.token) {
    return cb(new Error('Kubernetes auth token not found'))
  }
  opts = opts || {}
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  var fullUrl = null
  if (opts.template && opts.template.metadata.namespace) {
    var namespace = opts.template.metadata.namespace
    fullUrl = urljoin(this.baseUrl, 'namespaces', namespace, this.name)
  } else {
    fullUrl = urljoin(this.baseUrl, this.name)
  }
  fullUrl = this._query(fullUrl, opts)
  var rsp = request(_.merge({ url: fullUrl }, this._requestOpts()))
  var processed = this._processResponse(opts, rsp)
  processed.on('error', function (err) {
    return cb(err)
  })
  var handleItems = function (items) { 
    return cb(null, items)
  }
  var concatStream = concat({ encoding: 'object' }, handleItems)
  processed.pipe(concatStream)
}

/**
 * Creates a resource according to the template
 *
 * Calls cb with an error if the resource already exists (or fails to be created), or with 
 * the created resource if the operation succeeds
 *
 * @param {object} opts - object containing the template to create on the cluster and options
 * @param {function} cb - callback(err, resource)
 */
ResourceClient.prototype.create = function (opts, cb) {
  if (!this.token) {
    return cb(new Error('Kubernetes auth token not found'))
  }
  var self = this
  var template = opts.template
  var kind = template.kind
  if (!template || !kind) {
    return cb(new Error('template must exist and must contain a Kind property'))
  }

  var checkIfExists = function (next) {
    if (opts.force) {
      return next(null)
    }
    self.get({ template: template }, function (err, items) {
      if (err) return next(err)
      if (items.length > 0) return next(new Error('resource already exists -- cannot create'))
      return next(null)
    })
  }

  var createResource = function (next) {
    var nsUrl = urljoin(self.baseUrl, 'namespaces')
    var fullUrl = nsUrl
    if (kind !== 'Namespace') {
      var namespace = _.get(template, 'metadata.namespace')
      if (!namespace) {
        return next(new Error('must specify a namespace in the resource template'))
      }
      fullUrl = urljoin(self.baseUrl, 'namespaces', namespace, self.name)
    }
    var reqParams = _.merge({
      url: fullUrl,
      method: 'POST',
      body: JSON.stringify(template)
    }, self._requestOpts())
    var processed = self._processResponse({}, request(reqParams))
    processed.on('error', function (err) {
      return next(err)
    })
    processed.on('data', function (data) {
      return next(null, data) 
    })
  }

  async.series([
    checkIfExists,
    createResource
  ], function (err, resource) {
    if (err) return cb(err) 
    return cb(null, resource)
  })
}

/**
 * Deletes a resource according to the template
 *
 * Calls cb with an error if the resource does not exist (or fails to be deleted), or with 
 * the deleted resource if the operation succeeds
 *
 * @param {object} opts - object containing the template to delete and options
 * @param {function} cb - callback(err, resource)
 */
ResourceClient.prototype.delete = function (opts, cb) {
  if (!this.token) {
    return cb(new Error('Kubernetes auth token not found'))
  }
  var self = this
  var template = opts.template || {}
  var name = opts.name || _.get(template, 'metadata.name')
  if (!name) {
    return cb(new Error('must specify the name of the resource to delete'))
  }

  var checkIfNotExists = function (next) {
    if (template === {}) {
      template = {
        metadata: {
          name: name
        }
      }
    }
    if (opts.force) {
      return next(null)
    }
    self.get({ template: template }, function (err, items) {
      if (err) return next(err)
      if (!(items.length > 0)) return next(new Error('resource does not exist -- cannot delete'))
      return next(null)
    })
  }

  var deleteResource = function (next) {
    var nsUrl = urljoin(self.baseUrl, 'namespaces')
    if (!name) {
      return next(new Error('must specify a name in the resource template'))
    }

    var namespace = null
    var fullUrl = null
    if (self.name !== 'namespaces') {
      namespace = opts.namespace || _.get(template, 'metadata.namespace') 
      fullUrl = urljoin(self.baseUrl, 'namespaces', namespace, self.name, name)
    } else  {
      fullUrl = urljoin(self.baseUrl, 'namespaces', name)
    }

    var reqParams = _.merge({
      url: fullUrl,
      method: 'DELETE',
    }, self._requestOpts())
    var processed = self._processResponse({}, request(reqParams))
    processed.on('error', function (err) {
      return next(err)
    })
    processed.on('data', function (data) {
      return next(null, data) 
    })
  }

  async.series([
    checkIfNotExists,
    deleteResource
  ], function (err, resource) {
    if (err) return cb(err) 
    return cb(null, resource)
  })
}

/**
 * Updates a resource on the cluster by applying a delta to an existing resource
 *
 * @param {object} old - the old template that will be updated
 * @param {object} delta - the update to apply to old
 * @param {function} cb - callback(err, resource)
 */
ResourceClient.prototype.update = function (old, delta, cb) {
  if (!this.token) {
    return cb(new Error('Kubernetes auth token not found'))
  }
  var self = this
  var newResource = _.assign({}, old, delta)
  var deleteResource = function (next) {
    self.delete(old, function (err, res) {
      if (err) return next(err)
      return next(null)
    })
  }
  var createResource = function (next) {
    self.create(newResource, function (err, res) {
      if (err) return next(err)
      return next(null, res)
    })
  }
  async.series([
    deleteOld,
    createNew
  ], function (err, res) {
    if (err) return cb(err)
    return cb(null, res)
  })
}

/**
 * Return a watch stream that's subscribed to this resource
 *
 * @param {object} opts - optional filters or labels
 */
ResourceClient.prototype.watch = function (opts) {
  if (!this.token) {
    return cb(new Error('Kubernetes auth token not found'))
  }
  opts = opts || {}
  var self = this
  var fullUrl = this._query(urljoin(this.baseUrl, 'watch', this.name), opts)
  var processed = this._processResponse(opts, request.get(fullUrl))
  return processed
}

/**
 * Long-poll an endpoint until a condition is met
 *
 * TODO: move to a separate module
 *
 * @param {object} opts - options dictionary containing a condition after which to invoke cb
 * @param {function} cb - callback(err, resources)
 */
ResourceClient.prototype.when = function (opts, cb) {
  if (!this.token) {
    return cb(new Error('Kubernetes auth token not found'))
  }
  opts = opts || {}
  var self = this
  var condition = opts.condition
  var times = opts.times || 60
  var interval = opts.interval || 1000
  if (!condition) {
    cb(new Error('must specify a condition inside when'))
  }
  async.retry({
    times: times,
    interval: interval
  }, function (next) {
    self.get(opts, function (err, resources) {
      if (err) return next(err)
      var match = condition(resources)
      if (match) {
        return next(null, match)
      }
      return next(new Error('condition not met'), null)
    })
  }, function (err, results) {
    if (err) return cb(err)
    return cb(null, results)
  })
}

/**
 * Provide a base resource (pod, service...), a state change (delta) to be applied to that resource,
 * and an action that should produce that state change, and calls cb once the cluster state 
 * contains a resource with that new state
 *
 * Action must be another operation on the ResourceClient (i.e. client.pods.create) using the 
 * same resource type 
 *
 * TODO: move to a separate module
 *
 * @param {object} opts - options dictionary containing the desired state, delta and action
 * @param {function} cb - callback(err, state)
 */
ResourceClient.prototype.update = function (opts, cb) {
  if (!this.token) {
    return cb(new Error('Kubernetes auth token not found'))
  }
  if (!opts.state || !opts.action || !opts.delta) {
    throw new Error('state, action, and delta must be specified in the options dictionary')
  }
  var newState = _.merge({}, opts.state, opts.delta)
  var actionOpts = opts.actionOpts || {}
  _.merge(actionOpts, { template: opts.state })
  var self = this
  this.get({
    template: newState
  }, function (err, resources) {
    if (err) return cb(err)
    if (resources.length > 0) {
      // the cluster is already in the correct state 
      return cb(null, resources)
    }
    // the cluster is not currently in the correct state -- perform the action
    self.when({
      condition: function (resources) {
        var withoutKind = _.omit(newState, 'kind')
        var match = _.find(resources, withoutKind)
        if (match) {
          return match
        }
      }
    }, function (err, resources) {
        if (err) return cb(err)
        return cb(null, resources)
    })
    console.log('waiting for: ' + JSON.stringify(newState))
    // perform the action and wait for the above 'when' operation to complete
    opts.action.bind(self)(actionOpts, function (err, cb) {
      if (err) return cb(err)
    })
  })
}

module.exports = function KubeClient(opts) {
  opts = opts || {}
  var protocol = settings.protocol,
    kubeHost = settings.kubeHost,
    kubePort = settings.kubePort,
    kubeVersion = settings.kubeVersion
  var defaultUrl = '{0}://{1}:{2}/api/{3}/'.format(protocol, kubeHost, kubePort, kubeVersion)
  this.baseUrl = opts.baseUrl || defaultUrl
  this.token = util.kubeAuthToken() 
  if (!this.token) {
    console.error('Kubernetes auth token not found')
  }
  var self = this
  _.forEach(['pods', 'services', 'replicationControllers', 'events', 'namespaces', 'nodes'], function (name) {
    self[name] = new ResourceClient({ name: name, baseUrl: self.baseUrl, token: self.token })
  })
}
