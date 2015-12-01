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

module.exports = {
  kubeAuthToken: kubeAuthToken
}
