var QQ = require('..')
  , qq = new QQ({removeCompleted:false})

var job = {
   shiner: 'face'
  ,meat: 'mouth'
}


qq.push('email', job, function(err) {
  console.log(err, 'adsfasdfasdf')
})



