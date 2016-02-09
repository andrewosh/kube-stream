var fs = require('fs')
var path = require('path')

var _ = require('lodash')
var yaml = require('js-yaml')

/**
 * Searches the .kube directory for an auth token
 * @param {function} cb - callback(err, token)
 */
var kubeAuthToken = function () {
  try { 
    var doc = fs.readFileSync(path.join(process.env.HOME, '.kube', 'config'))
    var token = _.get(yaml.safeLoad(doc), 'users[0].user.token')
    return token
  } catch (err) {
    return null
  }
}

/**
 * Enumerates all valid paths in an object
 * @param {object} obj - the object
 */
var enumeratePaths = function (obj) {
  var paths = []
  var _pathHelper = function (path, node) {
    if (node && typeof(node) !== 'object') {
      paths.push(path)
    } else if (node) { 
      return _.keys(node).map(function (child) {
        return _pathHelper(path + '/' + child, node[child])
      })
    }
  }
  _pathHelper('', obj)
  return paths
}

module.exports = {
  kubeAuthToken: kubeAuthToken,
  enumeratePaths: enumeratePaths
}
