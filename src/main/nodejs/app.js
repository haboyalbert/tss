var express = require('express');
var app = module.exports = express.createServer();
var esClient=require('./elasticSearch')

//use get temp instead of post for debug
app.get('/queryByGeo', function (req, res) {
    esClient.test(req,res)
})

var server = app.listen(8081, function () {

  var host = server.address().address
  var port = server.address().port

  console.log("server starts at http://%s:%s", host, port)

})
