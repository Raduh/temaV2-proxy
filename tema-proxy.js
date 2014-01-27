/*

Copyright (C) 2010-2013 KWARC Group <kwarc.info>

This file is part of TeMaSearch.

MathWebSearch is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

MathWebSearch is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with TeMaSearch.  If not, see <http://www.gnu.org/licenses/>.

*/

var http = require("http");
var url = require('url');

var TEMA_PROXY_PORT = 8889;

var ES_HOST = "localhost";
var ES_PORT = 9200;
var MWS_HOST = "localhost";
var MWS_PORT = 9090;

var MAX_MWS_IDS = 1000;
var MAX_DOC_SIZE_CHARS = 10000;

http.createServer(function(request, response) {
    var url_parts = url.parse(request.url, true);
    var q = url_parts.query;
    var tema_text = q.text || "";
    var tema_math = q.math || "";
    var tema_from = q.from || 0;
    var tema_size = q.size || 10;

    var send_response = function(status_code, json_response) {
        if (status_code >= 500) {
            console.log(json_response);
        }
        response.writeHead(status_code, {
            "Content-Type" : "application/json; charset=utf-8",
            "Access-Control-Allow-Origin" : "*"
        });
        response.write(JSON.stringify(json_response));
        response.end();
    };

    var es_response_handler = function(es_response) {
        send_response(200, es_response);
    }

    var es_error_handler = function(error) {
        error.tema_component = "elasticsearch";
        send_response(500, error);
    };

    var mws_error_handler = function(error) {
        error.tema_component = "mws";
        send_response(error.status_code, error);
    };

    if (tema_math == "") {
        es_query(tema_text, null, tema_from, tema_size,
                 es_response_handler, es_error_handler);
    } else {
        mws_query(tema_math, MAX_MWS_IDS, function(mws_response) {
            var mws_ids = mws_response.data;
            es_query(tema_text, mws_ids, tema_from, tema_size,
                     es_response_handler, es_error_handler);
        }, mws_error_handler);
    }
}).listen(TEMA_PROXY_PORT);


/**
 * @callback result_callback(json_data)
 */
var es_query =
function(query_str, mws_ids, from, size, result_callback, error_callback) {
    var bool_must_filters = [];
    if (query_str.trim() != "") {
        bool_must_filters.push({
            "match" : {
                "xhtml" : {
                    "query" : query_str,
                    "operator" : "and"
                }
            }
        });
    }
    if (mws_ids != null) {
        bool_must_filters.push({
            "terms" : {
                "ids" : mws_ids,
                "minimum_match" : 1
            }
        });
    }

    var esquery_data = JSON.stringify({
        "from" : from,
        "size" : size,
        "query" : {
            "bool" : {
                "must" : bool_must_filters
            }
        },
        /*
        "highlight" :  {
            "pre_tags" : ["<div class=\"text-highlight\">"],
            "post_tags" : ["</div>"],
            "fields" : {
                "xhtml" : {
                    "number_of_fragments" : 0,  // return non-fragmented source
                    "no_match_size" : MAX_DOC_SIZE_CHARS
                        // return even if there is no highlight
                }
            }
        },
        */
        "fields" : [ "_source.id_mappings", "_source.xhtml" ]
    });
    var esquery_options = {
        hostname: ES_HOST,
        port: ES_PORT,
        path: '/tema-search/doc/_search',
        method: 'GET',
        headers: {
            'Content-Type': 'application/xml',
            'Content-Length': Buffer.byteLength(esquery_data, 'utf8')
        }
    };

    var req = http.request(esquery_options, function(response) {
        if (response.statusCode == 200) {
            var raw_data = '';
            response.on('data', function (chunk) {
                raw_data += chunk;
            });
            response.on('end', function () {
                var json_data = JSON.parse(raw_data);
                var json_wrapped_data = wrap_es_result(json_data, query_str, mws_ids);
                if (json_wrapped_data != null) {
                    result_callback(json_wrapped_data);
                } else {
                    result_callback(json_data);
                }
            });
        } else {
            var raw_data = '';
            response.on('data', function (chunk) {
                raw_data += chunk;
            });
            response.on('end', function () {
                var json_data = JSON.parse(raw_data);
                error_callback(json_data);
            });
        }
    });

    req.on('error', function(error) {
        error_callback(error);
    });

    req.write(esquery_data);
    req.end();
};

/**
 * @callback result_callback(json_data)
 */
var mws_query =
function(query_str, limit, result_callback, error_callback) {
    var mwsquery_data =
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
            'Content-Length': Buffer.byteLength(mwsquery_data, 'utf8')
        }
    };

    var req = http.request(mwsquery_options, function(response) {
        if (response.statusCode == 200) {
            var raw_data = '';
            response.on('data', function (chunk) {
                raw_data += chunk;
            });
            response.on('end', function () {
                var json_data = JSON.parse(raw_data);
                result_callback(json_data);
            });
        } else {
            var raw_data = '';
            response.on('data', function (chunk) {
                raw_data += chunk;
            });
            response.on('end', function () {
                var json_data = {
                    status_code : response.statusCode,
                    data : raw_data
                };
                error_callback(json_data);
            });
        }
    });

    req.on('error', function(error) {
        error.status_code = 500;
        error_callback(error);
    });

    req.write(mwsquery_data);
    req.end();
};

var wrap_es_result = function(es_result, query_str, mws_ids) {
    try {
        var hits = [];
        for (var i = 0; i < es_result.hits.hits.length; i++) {
            var xhtml = es_result.hits.hits[i].fields['_source.xhtml'];
            var math_ids = [];
            if (mws_ids != null) {
                var all_math_ids =
                    es_result.hits.hits[i].fields['_source.id_mappings'];
                for (var j = 0; j < all_math_ids.length; j++) {
                    if (mws_ids.indexOf(all_math_ids[j].id) > -1) {
                        math_ids.push({
                            "url" : all_math_ids[j].url,
                            "xpath" : all_math_ids[j].xpath
                        });
                    }
                }
            }
            hits.push({
                "text" : query_str.split(' ').filter(
                    function(x) { return x != ''; }
                    ),
                "math_ids" : math_ids,
                "xhtml" : xhtml
            });
        }

        var result = {
            "took" : es_result.took,
            "timed_out" : es_result.timed_out,
            "total" : es_result.hits.total,
            "hits" : hits
        };

        return result;
    } catch (e) {
        console.log(e);
        return null;
    }
};
