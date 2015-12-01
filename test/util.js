var format = require('string-format') 
format.extend(String.prototype)

var settings = require('../config/main.js')
var KubeStream = require('../index.js')

var protocol = settings.protocol
var host = settings.kubeHost
var port = settings.kubePort
var version = settings.kubeVersion
var baseUrl = '{0}://{1}:{2}/api/{3}'.format(protocol, host, port, version)

var makePod = function (namespace, name) {
  return {
    kind: 'Pod',
    metadata: {
      name: name,
      namespace: namespace,
      labels: {
        name: name
      }
    },
    spec: {
      containers: [
        {
          name: 'demo-container',
          image: 'ubuntu:latest',
          imagePullPolicy: 'Always',
        }
      ],
      dnsPolicy: 'ClusterFirst',
      restartPolicy: 'Always'
    }
  }
}

var makeNamespace = function (name) {
  return {
    kind: 'Namespace',
    metadata: {
      name: name,
      labels: {
        name: name
      }
    }
  }
}

var makeService = function (namespace, name, pod) {
  return {
    kind: 'Service',
    metadata: {
      name: name,
      namespace: namespace,
      labels: {
        name: name
      } 
    }, 
    spec: {
      selector: {
        name: pod
      },
      ports: [
        {
          port: 80,
          targetPort: 80,
          protocol: 'TCP'
        }
      ],
      type: 'NodePort',
      sessionAffinity: 'None'
    }
  }
}


module.exports = {
  baseUrl: baseUrl,
  makePod: makePod,
  makeNamespace: makeNamespace,
  makeService: makeService
}
