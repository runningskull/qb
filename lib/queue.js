var Events = require('events').EventEmitter
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
 *   - `redisClient` - a redis client for QQ to reuse
 *   - `redis` - redis configuration:
 *      - `host`: defaults to `localhost`
 *      - `port`: defaults to 6379
 *      - `user`: authenticate as user
 *      - `pass`: authentication password
 *   - `prefix`: prefix for redis keys. defaults to 'qq'
 *   - `keysep`: redis key separator. defaults to ':'
 *   - `removeCompleted`: erase completed jobs. defaults to `true`
 *
 * @param {Object} options
 */

function QQ(options) {
  if (! (this instanceof QQ))
    return new QQ(options);

  _.bindAll(this, '_key', 'push', 'process', 'shutdown')

  options.redis || (options.redis = {})

  options = _.extend({
     redis: {}
    ,removeCompleted: true
    ,keysep: ':'
    ,prefix: 'QQ'
  }, options)

  this.options = options

  // One primary client that's never used for popping the queue
  this.R = options.redisClient
    ? options.redisClient
    : redis.client(options.redis.port, options.redis.host)

  this.clients = {}
  this.workers = {}
}

util.inherits(QQ, Events)
proto = QQ.prototype


/**
 * Generate a key by prefixing this queue's prefix
 */
proto._key = function(arr) {
  arr.unshift(this.options.prefix)
  return arr.join(this.options.keysep)
}


/**
 * Push a job into the queue
 *
 * If a job with the passed `id` already exists, it will be updated.
 * If no `id` is passed, the default is an incrementing integer.
 *
 * @param {String} type
 * @param {Object} jobData
 * @param {String} id [optional] A unique job ID
 * @api public
 */
proto.push = function(jobType, jobData, id, $done) {
  var _push = _.bind(function() {
    this.R.multi()
      .lpush(this._key(['~', 'waiting', jobType]), id)
      .set(this._key(['job', jobType, id]), str)
      .exec($done)
  }, this)

  // `id` is optional
  if (!$done)
    $done=id, id=null;

  // msgpack can thros
  try{ var str = msgpack.pack(jobData) }
  catch(ex){ return $done(ex) }
 
  // If ID is already set, we don't have to generate one
  if (id) return _push();

  // Otherwise, increment our counter & use it as the ID
  this.R.incr(this._key(['#', jobType]), function(err, _id) {
    if (err) return $done(err);
    id = _id
    _push()
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

  n || (n = 1)

  // Create a new redis client for this job type
  this.clients[jobType] = redis.createClient(this.options.redis.port,
                                             this.options.redis.host)

  // Concurrent workers
  while (this.workers[jobType].length < n)
    this.workers[jobType].push(new Worker(jobType, this, fn));

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

exports = module.exports = QQ

