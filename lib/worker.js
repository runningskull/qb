var msgpack = require('msgpack')
  , _ = require('underscore')
  , async = require('async')
  , util = require('util')

  , proto   // worker prototype


function Worker(jobType, qb, fn) {
  _.bindAll(this, 'start', 'work', 'stop', '_fail', '_complete')

  this.type = jobType
  this.qb = qb
  this.client = qb.clients[jobType]
  this.fn = fn

  var mkkey = function(lst){ return qb._key(['~', lst, jobType]) }

  this.keys = _.reduce(['waiting', 'active', 'failed', 'complete'],
                       function(m,v){ return (m[v]=mkkey(v)), m }, {})
}

proto = Worker.prototype


proto.start = function() {
  var _fetch = _.bind(function(jobId, $next) {
    var key = new Buffer(this.qb._key(['job', this.type, jobId]))
      , fin = function(err,str){ $next(err, jobId, str) }

    this._working = true
    this.client.get(key, function(err,str){ $next(err, jobId, str) })
  }, this)

  var _parse = _.bind(function(jobId, jobStr, $next) {
    try{ this.job = msgpack.unpack(new Buffer(jobStr)) }
    catch(ex){ return $next(ex) }

    this.job._id = jobId
    $next()
  }, this)

  // Wait for a job to become available
  var _pop = _.bind(this.client.brpoplpush, this.client, 
                    this.keys.waiting, this.keys.active, 0)

  async.waterfall([_pop, _fetch, _parse], this.work)
  return this
}


proto._fail = function(err, $done) {
  try{ var jobStr = msgpack.pack(job) }
  catch(ex){ throw ex }

  var _job = _.clone(this.job)
    , jobId = this.job._id
  delete _job._id

  // Save updates to the job
  this.qb.R.set(this.qb._key(['job', this.type, jobId]), jobStr)

  // Move it to failed list
  this.qb.R.multi()
    .lrem(this.keys.active, 1, jobId)
    .lpush(this.keys.failed, jobId)
    .exec($done)
}


proto._complete = function() {
  var jobId = this.job._id

  if (!this.qb.options.removeCompleted) {   // Keep a record of completed jobs
    this.qb.R.multi()
      .lrem(this.keys.active, 1, jobId)
      .lpush(this.keys.complete, jobId)
      .exec()

  } else {                                  // Remove completed jobs entirely
    this.qb.R.lrem(this.keys.active, 1, jobId)
    this.qb.R.del(this.qb._key(['job', this.type, jobId]))
  }
}

proto.work = function(err, job) {
  // Error fetching/parsing job from queue
  if (err) {
    this._working = false
    // TODO: how to handle this error?
    return this._stopped ? null : this.start()
  }

  // Process the job, failing it on error
  this.fn(this.job, _.bind(function(_err) {
    if (_err) this._fail(_err);
    else this._complete();

    this.job = null
    this._working = false
    return this._stopped ? null : this.start()
  }, this))
  return this
}


proto.stop = function($done) {
  if (! this._working) $done();
  else this._stopped = true

  return this
}



/**
 * Expose Worker
 */

exports = module.exports = Worker

