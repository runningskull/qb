var redis = require('redis')

var opts = {
  detect_buffers: true
}

exports._clients = {}

exports.client = function(port, host) {
  return exports._clients[port + host]
    || (exports._clients[port + host] = redis.createClient(port, host, opts))
}


exports.createClient = function(port, host) {
  return redis.createClient(port, host, opts)
}

