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
  if (!this.token) {
    return {}
  }
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

ResourceClient.prototype._urlFromOpts = function (opts) {
  var self = this
  var template = opts.template
  var name = opts.name || _.get(template, 'metadata.name')
  var nsUrl = urljoin(self.baseUrl, 'namespaces')
  if (!name) {
    return next(new Error('must specify a name in the resource template'))
  }

  var fullUrl = null
  if (self.name !== 'namespaces') {
    var namespace = _.get(template, 'metadata.namespace') 
    if (!namespace) {
      return next(new Error('must specify a namespace (contained in a template)'))
    }
    fullUrl = urljoin(self.baseUrl, 'namespaces', namespace, self.name, name)
  } else  {
    fullUrl = urljoin(self.baseUrl, 'namespaces', name)
  }
  return fullUrl
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
        //console.log('toMatch: ' + JSON.stringify(toMatch, null, ' '))
        //console.log('tempNoKind: '  + JSON.stringify(tempNoKind, null, ' '))
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

/**
 * Gets the list of a type of resource on the cluster
 *
 * Calls cb with an error if the get request fails, or with the list of resources if it 
 * succeeds
 *
 * @param {object} opts - object containing filters for the get request
 * @param {function} cb - callback(err, resources)
 */
ResourceClient.prototype.get = function (opts, cb) {
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
 * Patch an existing resource
 *
 * The patch type (opts.type) must be one of the following:
 *  1) add - adds to the resource (RFC6902)
 *  2) replace - replaces the resource (RFC6902)
 *  3) remove - removes the resource (RFC6902)
 *  4) merge - performs a Strategic Merge Patch (Kubernetes-specific) with the patch/resource
 *
 * Calls cb with an error if the patch request fails, or with the patched resource if it 
 * succeeds
 *
 * @param {object} opts - object containing the template to patch
 * @param {function} cb - callback(err, patched)
 */
ResourceClient.prototype.patch = function (opts, cb) {
  var self = this 
  var template = opts.template
  var name = opts.name || _.get(template, 'metadata.name')
  var patch = opts.patch
  var type = opts.type
  if (!name || !patch || !type) {
    return cb(new Error('name and patch both must exist'))
  }

  var checkIfExists = function (next) {
    if (template === {}) {
      template = {
        metadata: {
          name: name
        }
      }
    }
    self.get({ template: template }, function (err, items) {
      if (err) return next(err) 
      if (items.length === 0) return next(new Error('resource does not exist -- cannot patch'))
      return next(null)
    })
  }

  var patchResource = function (next) {
    var fullUrl = self._urlFromOpts(opts)
    var reqParams = _.merge({
      url: fullUrl,
      method: 'PATCH',
    }, self._requestOpts())
    var body = null 
    if (type === 'merge') {
      body = patch
      reqParams.headers['Content-Type'] = 'application/strategic-merge-patch+json'
    } else {
      var paths = util.enumeratePaths(patch)
      if (paths.length !== 1) {
        return next(new Error('patch object can only contain a single valid path'))
      }
      var path = paths[0]
      body = {
        op: type,
        path: path,
        value: _.get(patch, path.split('/').join('.'))
      }
      console.log('body is: ' + body)
      reqParams.headers['Content-Type'] = 'application/json-patch+json'
      reqParams.headers['If-Match'] = 'abc123'
    }
    reqParams.json = body
    console.log('reqParams: ' + JSON.stringify(reqParams))
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
    patchResource
  ], function (err, patched) {
    if (err) return cb(err)
    return cb(null, patched)
  })
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
    var fullUrl = self._urlFromOpts(opts)
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
        if (!_.isArray(match) || match.length !== 0) { 
          return next(null, match)
        }
      }
      return next('condition not met')
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
 * options:
 *  - state {object}: the resource to update (i.e. a Pod)
 *  - action {function}: the action to take on the resource (i.e. client.pods.create)
 *  - delta {object}: the desired state change after the action was taken (i.e. the Pod is running)
 *  - condition {function): a predicate to match against resources 
 *
 * Either a delta or a condition must be specified, but not both
 *
 * @param {object} opts - options dictionary containing the desired state, delta and action
 * @param {function} cb - callback(err, state)
 */
ResourceClient.prototype.changeState = function (opts, cb) {
  if (!opts.state || !opts.action) {
    throw new Error('state and action must be specified')
  }
  if (!opts.condition && !opts.delta) {
    throw new Error('did not specify either a condition or a delta')
  }
  if (opts.condition && opts.delta) {
    throw new Error('cannot specify both a condition and a delta')
  }
  var actionOpts = opts.actionOpts || {}
  _.merge(actionOpts, { template: opts.state })

  var newState = null
  if (opts.delta) { 
    newState = _.merge({}, opts.state, opts.delta)
  }

  var condition = function (resources) {
    var cond = null
    if (newState) {
      cond = _.omit(newState, 'kind')
      return _.find(resources, cond)
    } else {
      return opts.condition(resources)
    }
  }

  var self = this
  var getOptions = opts.state ? { template: opts.state } : {}
  this.get(getOptions, function (err, resources) {
    if (err) return cb(err)
    var match = condition(resources)
    if (match) {
      if (!_.isArray(match) || match.length !== 0) { 
      // the cluster is already in the correct state
        return cb(null, match)
      }
    }
    // the cluster is not currently in the correct state -- perform the action
    var whenOpts = {}
    if (opts.interval) {
      whenOpts.interval = opts.interval
    } 
    if (opts.times) {
      whenOpts.times = opts.times
    }
    self.when(_.merge(whenOpts, getOptions, { condition: condition }), 
      function (err, resource) {
        if (err) return cb(err)
        return cb(null, resource)
    })
    // perform the action and wait for the above 'when' operation to complete
    opts.action.bind(self)(actionOpts, function (err) {
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
    console.warn('Kubernetes auth token not found')
  }
  var self = this
  _.forEach(['pods', 'services', 'replicationControllers', 'events', 'namespaces', 'nodes'], function (name) {
    self[name] = new ResourceClient({ name: name, baseUrl: self.baseUrl, token: self.token })
  })
}
