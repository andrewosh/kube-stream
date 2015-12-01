var _ = require('lodash')
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
module.exports = KubeStream = function (opts) {
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

KubeStream.prototype._query = function (url, opts) {
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

KubeStream.prototype._requestOpts = function () {
  return {
    headers: {
      'Authorization': 'Bearer {0}'.format(this.token)
    }
  }
}

KubeStream.prototype._extract = function (items, view) {
  if (!view) {
    return items
  } 
  return _.pluck(items, view)
}

KubeStream.prototype._processResponse = function (opts, rsp) {
  opts = opts || {}
  var self = this
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
          console.log('MATCHED!')
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
  if (!cb) {
    return processed
  } else {
    processed.on('error', function (err) {
      return cb(err)
    })
    var handleItems = function (items) { 
      console.log('in handleItems: ' + items)
      return cb(null, items)
    }
    var concatStream = concat(handleItems)
    processed.pipe(concatStream)
  }
}

/**
 * Creates a resource according to the template
 *
 * Returns a watch stream for the given template
 *
 * @param {object} template - the resource template to create on the cluster
 */
KubeStream.prototype.create = function (template) {
  var namespace = _.get(template, 'metadata.namespace')
  if (!template || !namespace) {
    throw new Error('template must exist and contain a namespace')
  }
  var getStream = this.get({
    template: template
  })
  getStream.pipe(es.through(function write(data) {
    if (data) {
      this.emit('error', new Error('pod matching template already exists'))
    }
  }, function end() {
    var fullUrl = urljoin(this.baseUrl, 'namespaces', namespace, this.name)
    var reqParams = _.merge({
      url: fullUrl,
      method: 'POST',
      body: JSON.stringify(template)
    }, this._requestOpts())
  })
  var merged = es.merge(
    templateStream,
    request(reqParams).pipe(es.through(function write(data) {
      if (_.isError(data)) {
        return this.emit('error', data)
      }}, function end() { 
        return this.emit('data', 'finished')
      }))
  )
  if (!cb) {
    return merged
  }
  merged.on('error', function (err) {
    merged.destroy()
    return cb(err)
  })
  merged.on('data', function (data) {
    if (data === 'finished') { 
      merged.destroy()
      return cb(null)
    }
  })
}


KubeStream.prototype.delete = function (template, cb) {
  var self = this
  var opts = {
    template: template
  }
  var stream = this.get(opts)
    .on('error', function (err) {
      return cb(err)
    })
    .on('data', function (items) {
      if (!items) {
        var err = new Error('no resources matching template found to delete')
        this.emit(err)
        if (cb) {
          return cb(err)
        }
      }
    })
    .pipe(es.map(function (data, next) {
      var url = self.baseUrl
      request(_.merge({
        url: url 
      }, this._requestOpts()))
    }))
}

KubeStream.prototype.update = function (template, opts, cb) {
}

KubeStream.prototype.watch = function (opts, cb) {
  opts = opts || {}
  var self = this
  var fullUrl = this._query(urljoin(this.baseUrl, 'watch', this.name), opts)
  console.log('fullUrl: ' + fullUrl)
  return this._processResponse(opts, request.get(fullUrl))
}

/**
 * Wraps a set of KubeStreams and emits all Kubernetes events
 * @constructor
function KubeStream(opts) {
    this.opts = opts || {}
    this.host = opts.host || settings.kube.proxyHost + ':' + settings.kube.proxyPort
    this.protocol = opts.protocol || settings.kube.protocol || 'http'
    this.version = opts.version || settings.kube.apiVersion || 'v1'

    var baseUrl = this.protocol + '://' + this.host + '/api/' + this.version + '/'
    var resources = ['pods', 'services', 'events', 'hosts', 'services', 'replicationControllers']
    _.forEach(resources, function (name) {
        this[name] = new KubeStream(baseUrl, name)
    })
}
**/

