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
module.exports = KubeClient = function (opts) {
  opts = opts || {}
  var protocol = settings.protocol,
    kubeHost = settings.kubeHost,
    kubePort = settings.kubePort,
    kubeVersion = settings.kubeVersion
  var defaultUrl = '{0}://{1}:{2}/api/{3}/'.format(protocol, kubeHost, kubePort, kubeVersion)

  this.baseUrl = opts.baseUrl || defaultUrl || 'http://localhost/api/v1'
  this.name = opts.name 

  this.token = util.kubeAuthToken() 
  if (!this.token) {
    throw new Error('Kubernetes auth token not found')
  }
}

KubeClient.prototype._query = function (url, opts) {
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

KubeClient.prototype._requestOpts = function () {
  return {
    headers: {
      'Authorization': 'Bearer {0}'.format(this.token)
    }
  }
}

KubeClient.prototype._extract = function (items, view) {
  if (!view) {
    return items
  } 
  return _.pluck(items, view)
}

KubeClient.prototype._processResponse = function (opts, rsp) {
  opts = opts || {}
  var self = this
  // TODO use pump here
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
}

KubeStream.prototype.get = function (opts, cb) {
  opts = opts || {}
  var fullUrl = this._query(urljoin(this.baseUrl, this.name), opts)
  var rsp = request(_.merge({ url: fullUrl }, this._requestOpts()))
  var processed = this._processResponse(opts, rsp)
  processed.on('error', function (err) {
    return cb(err)
  })
  var handleItems = function (items) { 
    return cb(null, items)
  }
  var concatStream = concat(handleItems)
  processed.pipe(concatStream)
}

/**
 * Creates a resource according to the template
 *
 * Calls cb with an error if the resource already exists (or fails to be created), or with 
 * the created resource if the operation succeeds
 *
 * @param {object} template - the resource template to create on the cluster
 * @param {function} cb - callback(err, resource)
 */
KubeStream.prototype.create = function (template, cb) {
  var self = this
  var namespace = _.get(template, 'metadata.namespace')
  if (!template || !namespace) {
    throw new Error('template must exist and contain a namespace')
  }

  var checkIfExists = function (next) {
    self.get({ template: template }, function (err, items) {
      if (err) return next(err)
      if (items) return next(new Error('resource already exists -- cannot create'))
      return next(null)
    })
  }

  var createResource = function (next) {
    var fullUrl = urljoin(self.baseUrl, 'namespaces', namespace, self.name)
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
 * @param {object} template - the resource template to delete on the cluster
 * @param {function} cb - callback(err, resource)
 */
KubeStream.prototype.delete = function (template, cb) {
  var self = this
  var namespace = _.get(template, 'metadata.namespace')
  if (!template || !namespace) {
    throw new Error('template must exist and contain a namespace')
  }

  var checkIfNotExists = function (next) {
    self.get({ template: template }, function (err, items) {
      if (err) return next(err)
      if (!items) return next(new Error('resource does not exist -- cannot delete'))
      return next(null)
    })
  }

  var deleteResource = function (next) {
    var fullUrl = urljoin(this.baseUrl, 'namespaces', namespace, self.name)
    var reqParams = _.merge({
      url: fullUrl,
      method: 'DELETE',
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
KubeStream.prototype.update = function (old, delta, cb) {
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

KubeStream.prototype.watch = function (opts, cb) {
  opts = opts || {}
  var self = this
  var fullUrl = this._query(urljoin(this.baseUrl, 'watch', this.name), opts)
  return this._processResponse(opts, request.get(fullUrl))
}
