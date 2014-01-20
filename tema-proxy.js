var http = require("http");

http.createServer(function(request, response) {
    /*
    mws_query('<mws:qvar/>', 30, function(mws_response) {
            // build es query
            var es_query_str = 

        }
    });
    */
    es_query('math', [212], 0, 5, null);

    response.writeHead(200, {"Content-Type": "text/plain"});
    response.write("Hello World");
    response.end();
}).listen(8888);

/**
 * @callback result_callback(json_data)
 */
var es_query = function(query_str, mws_ids, from, size, result_callback) {
    var query = {
        "from" : from,
        "size" : size,
        "query" : {
            "bool" : {
                "must" : [{
                    "terms" : {
                        "ids" : mws_ids,
                        minimum_match : 1
                    }
                }, {
                    "match" : {
                        "xhtml" : query_str
                    }
                }]
            }
        }
    };
    var get_data = JSON.stringify(query);
    var esquery_options = {
        hostname: 'localhost',
        port: 9200,
        path: '/tema-search/doc/_search',
        method: 'GET',
        headers: {
            'Content-Type': 'application/xml',
            'Content-Length': get_data.length
        }
    };

    var req = http.request(esquery_options, function(res) {
        console.log(res.statusCode);
        if (res.statusCode == 200) {
            res.on('data', function (raw_data) {
                json_data = JSON.parse(raw_data);
                debugger;
                console.log(json_data);
                //result_callback(json_data);
            });
        }
    });

    req.on('error', function(error) {
        console.log(error);
    });

    req.write(get_data);
    req.end();
};

/**
 * @callback result_callback(json_data)
 */
var mws_query = function(query_str, limit, result_callback) {

    var post_data =
        '<mws:query' +
            ' limitmin="0"' +
            ' answsize="' + limit + '"' +
            ' totalreq="yes">' +
            '<mws:expr>' +
                query_str +
            '</mws:expr></mws:query>';

    var mwsquery_options = {
        hostname: 'localhost',
        port: 9090,
        path: '/',
        method: 'POST',
        headers: {
            'Content-Type': 'application/xml',
            'Content-Length': post_data.length
        }
    };

    var req = http.request(mwsquery_options, function(res) {
        if (res.statusCode == 200) {
            res.on('data', function (raw_data) {
                json_data = JSON.parse(raw_data);
                result_callback(json_data);
            });
        }
    });

    req.on('error', function(error) {
        console.log(error);
    });

    req.write(post_data);
    req.end();
};
