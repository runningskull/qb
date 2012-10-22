var Events = require('events').EventEmitter
  , Worker = require('./worker')
  , msgpack = require('msgpack')
  , redis = require('./redis')
  , _ = require('underscore')
  , async = require('async')
  , util = require('util')

  , proto   // queue prototype



function retryActive() {
}


/**
 * Create a new queue
 *
 * Options:
 *   - `redisClient` - a redis client for QB to reuse
 *   - `redis` - redis configuration:
 *      - `host`: defaults to `localhost`
 *      - `port`: defaults to 6379
 *      - `user`: authenticate as user
 *      - `pass`: authentication password
 *   - `prefix`: prefix for redis keys. defaults to 'qb'
 *   - `keysep`: redis key separator. defaults to ':'
 *   - `removeCompleted`: remove completed jobs from redis. defaults to true
 *
 * @param {Object} options
 */

function QB(options) {
  if (! (this instanceof QB))
    return new QB(options);

  _.bindAll(this, '_key', 'push', 'process', 'shutdown')

  options.redis || (options.redis = {})

  options = _.extend({
     redis: {}
    ,keysep: ':'
    ,removeCompleted: true
    ,prefix: 'qb'
  }, options)

  this.options = options

  // One primary client that's never used for popping the queue
  this.R = options.redisClient
    ? options.redisClient
    : redis.client(options.redis.port, options.redis.host)

  this.clients = {}
  this.workers = {}
}

util.inherits(QB, Events)
proto = QB.prototype


/**
 * Generate a key by prefixing this queue's prefix
 */
proto._key = function(arr) {
  arr.unshift(this.options.prefix)
  return arr.join(this.options.keysep)
}


proto._id = function() {
  return QB.__id = ((QB.__id||0)+1) % 0xFFFFFF
}


/**
 * Push a job into the queue
 *
 * @param {String} type
 * @param {Object} jobData
 * @api public
 */
proto.push = function(jobType, jobData, $done) {
  var _push = _.bind(function(id) {
    this.R.multi()
      .lpush(this._key(['~', 'waiting', jobType]), id)
      .set(this._key(['job', jobType, id]), str)
      .exec($done)
  }, this)

  // msgpack can throw
  try{ var str = msgpack.pack(jobData) }
  catch(ex){ return $done(ex) }

  this.R.incr(this._key(['~', '#', jobType]), function(err, id) {
    if (err) return $done(err);
    _push(id)
  })
}


/**
 *  `process()` can only be called once per jobType. Subsequent calls
 *  w/ the same `jobType` will be ignored
 *
 *  @param {String} jobType
 *  @param {Number} n [optional] Run `n` workers concurrently
 *  @param {Function} fn
 *  @api public
 */
proto.process = function(jobType, n, fn) {
  if (_.has(this.workers, jobType)) return;

  // `n` is optional
  if (!fn)
    fn=n, n=1;

  // Create a new redis client for this job type
  this.clients[jobType] = redis.createClient(this.options.redis.port,
                                             this.options.redis.host)

  // Concurrent workers
  this.workers[jobType] = []
  while (this.workers[jobType].length < n) {
    var w = new Worker(jobType, this, fn)
    this.workers[jobType].push(w.start())
  }

  return true
}



/**
 *  Wait for active jobs to finish, then stop processing jobs
 *
 *  @param {Function} $done
 *  @api public
 */
proto.shutdown = function($done) {
  function _stopWorkers(warr, jobType, $_done) {
    // Safely kill any brpoplpush's that are waiting
    delete this.clients[jobType]

    // Stop each worker
    return _.bind(async.parallel, null, _.pluck(warr, 'stop'))
  }

  async.parallel(_.map(this.workers, _stopWorkers), $done)
}


/**
 * Expose the queue
 */

exports = module.exports = QB

