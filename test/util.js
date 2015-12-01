var format = require('string-format') 
format.extend(String.prototype)

var settings = require('../config/main.js')
var KubeStream = require('../index.js')

var protocol = settings.protocol
var host = settings.kubeHost
var port = settings.kubePort
var version = settings.kubeVersion
var baseUrl = '{0}://{1}:{2}/api/{3}'.format(protocol, host, port, version)

var makePod = function (num) {
  return {
    type: 'Pod',
    apiVersion: 'v1',
    metadata: {
      name: 'pod' + num,
      namespace: 'default',
      labels: {
        name: 'pod' + num
      }
    },
    spec: {
      containers: [
        {
          name: 'demo_container',
          image: 'ubuntu:latest'
        }
      ]
    }
  }
}


module.exports = {
  baseUrl: baseUrl,
  makePod: makePod
}
