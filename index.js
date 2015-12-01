var _ = require('lodash')
var request = require('request')
var es = require('event-stream')
var Bacon = require('bacon')
var urljoin = require('url-join')
var JSONStream = require('JSONStream')
var format = require('string-format')
format.extend(String.prototype)

var util = require('./lib/util.js')

/**
 * Manages all communication with a Kubernetes resource endpoint (i.e. 'pods')
 * @constructor
 */
module.exports = KubeStream = function (opts) {
  opts = opts || {}
  this.baseUrl = opts.baseUrl || util.baseUrl || 'http://localhost/api/v1'
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
      _.forEach(_.get(data, 'items'), function (item) {
        thr.emit('data', item)
      })
    }))
    .pipe(es.map(function (data, cb) {
      if (opts.template) {
        if (_.isMatch(data, opts.template)) { 
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
    processed.on('data', function (data) {
      return cb(null, data)
    })
    processed.on('error', function (err) {
      return cb(err)
    })
  }
}

KubeStream.prototype.create = function (template, cb) {
  var templateStream = this.watch({
    template: template
  })
  var fullUrl = urljoin(this.baseUrl, this.name)
  var reqParams = _.merge({
    url: fullUrl,
    method: 'POST',
    body: JSON.stringify(template)
  }, this._requestOpts())
  var merged = es.merge(
    templateStream,
    request(reqParams)).pipe(es.map(function (data, callback) {
      console.log('data: ' + data)
      if (_.isError(data)) {
        return callback(null, data)
      }
      return callback()
    }))
  if (!cb) {
    return merged
  }
  merged.on('error', function (err) {
    return cb(err)
  })
  merged.on('data', function (data) {
    return cb(null, data)
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
  var self = this
  var fullUrl = this._query(urljoin(this.baseUrl, this.name), opts)
  var watchUrl = urljoin(this.baseUrl, 'watch', this.name)
  return this._processResponse(opts, request.get(watchUrl))
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

