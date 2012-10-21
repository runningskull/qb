var msgpack = require('msgpack')
  , _ = require('underscore')
  , util = require('util')

  , proto   // worker prototype



function Worker(jobType, qq, fn) {
  _.bindAll(this, 'start', 'work', 'stop')

  this.type = jobType
  this.qq = qq
  this.client = qq.clients[jobType]
  this.fn = fn

  this.start()
}


proto.start = function() {
  var _fetch = _.bind(function(jobId, $next) {
    this._working = true
    $next = _.bind($next, null, jobId)
    this.client.get(this.qq.key(['job', this.type, jobId]), $next)
  }, this)

  var _parse = _.bind(function(jobId, jobStr, $next) {
    try{ var obj = msgpack.unpack(jobStr) }
    catch(ex){ return $next(ex) }

    obj._id = jobId
    $next(null, obj)
  }, this)

  var waiting = qq._key(['~', 'waiting', this.type])
    , active = qq._key(['~', 'active', this.type])
    
    // This will block until a job is available
    , _pop = _.bind(this.client.brpoplpush, this.client, active, 0)

  async.waterfall([_pop, _fetch, _parse], this.work)
}


proto.work = function(err, job) {
  var _fail = _.bind(function(_err, $done) {
    var id = job._id
    delete job._id

    // Save updates to the job and move it to 'failed'
    this.qq.R.multi()
      .set(this.qq.key(['job', this.type, job._id]), job)
      .lrem(this.qq.key(['~', 'active']), id)
      .lpush(this.qq.key(['~', 'failed']), id)
      .exec($done)
  }, this)

  var _complete = _.bind(function($done) {
    if (!this.qq.gc) {
      // Keep a record of completed jobs, but don't keep their data
      this.qq.R.del(this.qq.key(['job', id]))
      this.qq.R.multi()
        .lrem(this.qq.key(['~', 'active']), id)
        .lpush(this.qq.key(['~', 'complete']), id)
        .exec($done)

    } else {
      // Remove completed jobs entirely
      this.qq.R
        .lrem(this.qq.key(['~', 'active']), id)
        .del(this.qq.key(['job', id]))

      $done()
    }
  }, this)

  // Error fetching/parsing job from queue
  if (err) {
    this._working = false
    this.fn(err)    // TODO: how to handle this error?
    return this._stopped ? null : this.start()
  }

  // Process the job, failing it on error
  this.fn(null, jobData, function(_err) {
    if (_err) _fail(_err);
    else _complete();

    this._working = false
    return this._stopped ? null : this.start()
  })
}


proto.stop = function($done) {
  if (! this._working) return $done();
  this._stopped = true
}



/**
 * Expose Worker
 */

exports = module.exports = Worker

