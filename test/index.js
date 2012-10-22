require('longjohn')
var QB = require('..')
  , qb = new QB({removeCompleted:true})


function renderTemplate(tpl, ctx){ return 'Hi there!' }
function sendEmail(job, msg, $done){ $done() }


var job = {
   to: 'shiner@dog.com'
  ,template: 'reg-welcome'
}

qb.process('email', function(job, $done) {
  console.log(['Sending', job.template, 'email to', job.to, '...'].join(' '))

  var message = renderTemplate(job.template, job)

  sendEmail(job.to, message, function(err) {
    if (err) return $done(err);     // Fail the job
    return $done()                  // ... or mark complete
  })
})


qb.push('email', job)



