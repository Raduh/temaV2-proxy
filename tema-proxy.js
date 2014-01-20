var http = require("http");


var ES_HOST = "localhost";
var ES_PORT = 9200;

var MWS_HOST = "localhost";
var MWS_PORT = 9090;
var MAX_MWS_IDS = 100;

http.createServer(function(request, response) {
    var headers = request.headers;
    var tema_text = headers.text || "";
    var tema_math = headers.math || "<mws:qvar/>";
    var tema_from = headers.from || 0;
    var tema_size = headers.size || 10;

    mws_query(tema_math, MAX_MWS_IDS, function(mws_response) {
        var mws_ids = mws_response.data;
        es_query(tema_text, mws_ids, tema_from, tema_size, function(es_response) {
            response.writeHead(200, {"Content-Type": "application/json"});
            response.write(JSON.stringify(es_response));
            response.end();
        }, error_handler);
    }, error_handler);

    var error_handler = 
    function(error) {
        response.writeHead(500, {"Content-Type": "application/json"});
        response.write(JSON.stringify(error));
        response.end();
    };
}).listen(8888);


/**
 * @callback result_callback(json_data)
 */
var es_query =
function(query_str, mws_ids, from, size, result_callback, error_callback) {
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
                        "xhtml" : {
                            "query" : query_str,
                            "operator" : "and"
                        }
                    }
                }]
            }
        }
    };
    var get_data = JSON.stringify(query);
    var esquery_options = {
        hostname: ES_HOST,
        port: ES_PORT,
        path: '/tema-search/doc/_search',
        method: 'GET',
        headers: {
            'Content-Type': 'application/xml',
            'Content-Length': get_data.length
        }
    };

    var req = http.request(esquery_options, function(response) {
        if (response.statusCode == 200) {
            var raw_data = '';
            response.on('data', function (chunk) {
                raw_data += chunk;
            });
            response.on('end', function () {
                json_data = JSON.parse(raw_data);
                result_callback(json_data);
            });
        } else {
            var raw_data = '';
            response.on('data', function (chunk) {
                raw_data += chunk;
            });
            response.on('end', function () {
                json_data = JSON.parse(raw_data);
                error_callback(json_data);
            });
        }
    });

    req.on('error', function(error) {
        error_callback(error);
    });

    req.write(get_data);
    req.end();
};

/**
 * @callback result_callback(json_data)
 */
var mws_query =
function(query_str, limit, result_callback, error_callback) {

    var post_data =
        '<mws:query' +
            ' limitmin="0"' +
            ' answsize="' + limit + '"' +
            ' totalreq="yes">' +
            '<mws:expr>' +
                query_str +
            '</mws:expr></mws:query>';

    var mwsquery_options = {
        hostname: MWS_HOST,
        port: MWS_PORT,
        path: '/',
        method: 'POST',
        headers: {
            'Content-Type': 'application/xml',
            'Content-Length': post_data.length
        }
    };

    var req = http.request(mwsquery_options, function(response) {
        if (response.statusCode == 200) {
            var raw_data = '';
            response.on('data', function (chunk) {
                raw_data += chunk;
            });
            response.on('end', function () {
                json_data = JSON.parse(raw_data);
                result_callback(json_data);
            });
        }
    });

    req.on('error', function(error) {
        error_callback(error);
    });

    req.write(post_data);
    req.end();
};
