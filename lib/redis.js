var redis = require('redis')

exports._clients = {}

exports.client = function(port, host) {
  return exports._clients[port + host]
    || (exports._clients[port + host] = redis.createClient(port, host))
}


exports.createClient = redis.createClient

