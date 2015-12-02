var settings = require('../config/main.js')

var protocol = settings.protocol
var host = settings.kubeHost
var port = settings.kubePort
var version = settings.kubeVersion
var baseUrl = '{0}://{1}:{2}/api/{3}'.format(protocol, host, port, version)

module.exports = baseUrl

