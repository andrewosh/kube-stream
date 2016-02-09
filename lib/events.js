var _ = require('lodash')
var es = require('event-stream')

// through streams meant to process watch events matching a template

module.exports = {

  _isPhase: function (phase) {
    return es.through(function write(data) {
      var phs = _.get(data, 'object.status.phase')
      if (phs === phase) {
        this.emit('data', data.object)
      }
    })
  },

  isRunning: function () {
    return _isPhase('Running')
  },

  isStopped: function () {
    return _isPhase('Stopped')
  },

  hasState: function (state) {
    return es.through(function write(data) {
      console.log('checking data: ' + JSON.stringify(data))
      var matches = _.isMatch(data, state)
      if (matches) {
        this.emit('data', data)
      }
    })
  }
}
