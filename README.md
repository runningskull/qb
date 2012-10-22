QB - A job queue for node.js.
===============================================

Uses redis for persistence, and msgpack for serialization.
Intuitive, flexible, & fast.


## How Do I?

```bash
npm install qb
```

```javascript
var QB = require('qb')
  , qb = new QB()
```

### new QB([options])

Returns a new instance of a queue.

Options:
- **redisClient**: Pass this to re-use an existing redis client
- **redis**: Optionally, an object containing any of `host`, `port`, `user`, `pass`
- **prefix**: Prefix for all of QB's redis keys. Defaults to 'qb'
- **keysep**: Key separator for all of QB's redis keys. Defaults to ':'
- **removeCompleted**: Remove completed jobs entirely from redis. If `true`, the job data remains in redis


### qb.push(jobType, jobData, [id], $done)

Push a job into the queue.

```javascript
var job = {to:'foo@bar.com', template:'reg-welcome', name:'Foo Bar'}

qb.push('email', job, function(err, jobId, jobStr) {
  console.log(err, jobId, jobStr)
})
```

When a job is pushed, it is given an incrementing number as an ID (_note: this ID is contained in `qb:~:#:<jobType>`_).
That number is pushed into `qb:~:waiting:<jobType>` and the job's data (serialized by msgpack) is stored in `qb:job:<jobType>:<id>`


### qb.process(jobType, [n], fn)

Process `jobType` jobs from the queue, using `n` concurrent workers.

```javascript
qb.process('email', function(job, $done) {
  console.log(['Sending', job.template, 'email to', job.to, '...'].join(' '))

  sendEmail(job, function(err) {
    if (err) return $done(err);     // Fail the job
    return $done()                  // ... or mark complete
  })
})
```

If the job fails, its data remains in redis, and its ID is pushed into `qb:~:failed:<jobType>`.

If it succeeds, one of the following happens:
- If `removeCompleted` is `true`, the job's ID and data are removed from redis
- If `removeCompleted` is `false`, the job's ID is stored in `qb:~:completed:<jobType>` and the job data remains in redis


