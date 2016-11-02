(function () {
  'use strict';
  var Q   = require('q')
  const elasticsearch = require('elasticsearch');
  const client = new elasticsearch.Client({
    //we should templatelize the user/pass using something like salt map.jinja
    host: 'http://abc:test@52.211.0.138:9200',
    log: 'error'
  });

  const search = function(term, offset) {
    var strs = term.split(",");
    var deferred = Q.defer();
    //build the query using term received
    var query = {

      "bool": {
        "must": {
          "match": {
            "content": {
              "query": strs[3],
              "minimum_should_match": "1"
            }
          }
        },
        "filter": {
          "geo_distance": {
            "distance": strs[2],
            "location": {
              "lat": strs[0],
              "lon": strs[1]
            }
          }
        }
      }
    };
    client.search({
      "index": 'us_large_cities',
      "type": 'city',
      "body": {
        "size": 250,
        "from": (offset || 0) * 250,
        "query": query

      }
    }).then(function(result) {
          var ii = 0,
              hits_in, hits_out = [];
          hits_in = (result.hits || {}).hits || [];
          for (; ii < hits_in.length; ii++) {
            hits_out.push(hits_in[ii]._source);
          }
          deferred.resolve(hits_out);
        },
        deferred.reject);

    return deferred.promise;
  };


const   printRow = function(results,res,index) {
  var now = Date.parse(new Date());
  var recordNo = 0;
  var recordDate = '';
  var finalrow = '';
  var cc = results[index].content.split("@#@");
  //check the time
  for (; recordNo < cc.length; recordNo++) {
    var recordtimestamp = cc[recordNo].split("::");
    if (now - recordtimestamp[0] <= 3600 * 24 * 1000)
      recordDate = new Date(parseInt(recordtimestamp[0])).toLocaleString().replace(/:\d{1,2}$/, ' ');
    finalrow = "\r\n " +recordNo+"  "+ recordDate + "::" + recordtimestamp[1] + "\r\n" + finalrow;

  }
  res.write( finalrow);
};

  // only for testing purposes
  // all calls should be initiated through the module
  const searchMe = function searchMe(req,res) {
    var i=0;
    search(req.query.param)
    .then(results => {
    results.forEach((hit, index) =>
        //print the city first
    res.write(`\t${ ++index} -${hit.city}`));
    i++;
    printRow(results,res,i);
  }).catch(console.error);
  };

  module.exports = {
    searchMe
  };
} ());
