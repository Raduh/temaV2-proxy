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

var config = require('./config.js');
var es = require('./elasticsearch.js');

var assert = require('assert');
var async = require('async');
var http = require("http");
var url = require('url');
var util = require('util');
var qs = require('querystring');

var MATH_PREFIX = "math";

var DEBUG = true;

http.createServer(function(request, response) {
    var process_query = function (query) {
        var tema_text = query.text || "";
        var tema_math = query.math || "";
        var tema_from = query.from || 0;
        var tema_size = query.size || 10;

        var send_response = function(status_code, json_response) {
            if (status_code >= 500) {
                console.log(json_response);
                util.log(json_response);
            }
            response.writeHead(status_code, {
                "Content-Type" : "application/json; charset=utf-8",
                "Access-Control-Allow-Origin" : "*"
            });
            response.write(JSON.stringify(json_response));
            response.end();
        };

        var schema_error_handler = function(error) {
            error.tema_component = "schema";
            send_response(error.status_code, error);
        };

        var es_response_handler = function(es_response) {
            if (DEBUG) util.log(JSON.stringify(es_response));
            var mathExprs = [];
            var hits = es_response['hits'];
            for (var h in hits) {
                for (var m in hits[h]['maths']) {
                    mathExprs.push(hits[h]['maths'][m].source);
                }
            }

            var schema_res_callback = function(sch_response) {
                es_response['total_schemata'] = sch_response['total'];
                es_response['schemata'] = sch_response['schemata'];
                send_response(200, es_response);
            }

            schema_query(mathExprs, config.SCHEMA_DEPTH,
                config.SCHEMA_CUTOFF_MODE, config.SCHEMA_LIMIT,
                schema_res_callback, schema_error_handler);
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
            es_query(tema_text, null, null, tema_from, tema_size,
                es_response_handler, es_error_handler);
        } else {
            mws_query(tema_math, config.MAX_MWS_IDS, function(mws_response) {
                var mws_ids = mws_response['ids'];
                var mws_qvar_data = mws_response['qvars'];
                es_query(tema_text, mws_ids, mws_qvar_data, tema_from, tema_size,
                    es_response_handler, es_error_handler);
            }, mws_error_handler);
        }
    }

    if (request.method == "GET") {
        var url_parts = url.parse(request.url, true);
        process_query(url_parts.query);
    } else if (request.method == "POST") {
        var body = "";

        request.on("data", function (data) {
            body += data;
        });

        request.on("end", function () {
            var query = qs.parse(body);
            process_query(query);
        });

        request.on("error", function (e) {
            // TODO
        });
    }
}).listen(config.TEMA_PROXY_PORT);


/**
 * @callback result_callback(json_data)
 */
var es_query =
function(query_str, mws_ids, mws_qvar_data, from, size, result_callback, error_callback) {
    var source_filters = [];
    var bool_must_filters = [];
    if (query_str.trim() != "") {
        bool_must_filters.push({
            "match" : {
                "text" : {
                    "query" : query_str,
                    "minimum_should_match" : "2",
                    "operator" : "or"
                }
            }
        });
    }
    if (mws_ids != null) {
        bool_must_filters.push({
            "terms" : {
                "mws_ids" : mws_ids,
                "minimum_match" : 1
            }
        });
        mws_ids.map(function(id) {
            source_filters.push("mws_id." + id);
        });
    } else {
        source_filters = false;
    }

    var esquery = JSON.stringify({
        "from" : from,
        "size" : size,
        "query" : {
            "bool" : {
                "must" : bool_must_filters
            }
        },
        "_source" : source_filters
    });
    es.query(esquery, function(result) {
        var math_elems_per_doc = [];
        result.hits.hits.map(function(hit) {
            var math_elems = [];
            try {
                var mws_ids = hit._source.mws_id;
                for (var mws_id in mws_ids) {
                    var mws_id_data = mws_ids[mws_id];
                    for (var math_elem in mws_id_data) {
                        var simple_mathelem = simplify_mathelem(math_elem);
                        var xpath = mws_id_data[math_elem].xpath;
                        math_elems.push({ "math_id": simple_mathelem, "xpath": xpath});
                    }
                }
            } catch (e) {
                // ignore
            }
            math_elems_per_doc.push({"doc_id" : hit._id, "maths" : math_elems});
        });

        es_query_document_details(math_elems_per_doc, query_str, function(docs_arr) {
            result_callback({
                "total" : result.hits.total,
                "qvars" : mws_qvar_data,
                "hits" : docs_arr
            });
        }, error_callback);
    }, error_callback);
};

var es_query_document_details = function(docs_with_math, query_words, result_callback, error_callback) {
    var callbacks = [];
    docs_with_math.map(function(doc_data) {
        callbacks.push(function(callback) {
            var bool_must_filters = [{"match": {"_id": doc_data["doc_id"]}}];
            var source_filter = ["metadata"];
            if (query_words.trim() != "") {
                bool_must_filters.push({
                    "match" : {
                        "text" : {
                            "query" : query_words,
                            "minimum_should_match" : "2",
                            "operator" : "or"
                        }
                    }
                });
            }
            doc_data["maths"].map(function (math_elem) {
                bool_must_filters.push({
                    "match": {
                        "text": {
                            "query": MATH_PREFIX + math_elem.math_id,
                            "analyzer": "keyword"
                        }
                    }
                });
                source_filter.push("math." + math_elem.math_id);
            });

            var esquery = JSON.stringify({
                "from": 0,
                "size": 1,
                "query": {
                    "bool": {
                        "must": bool_must_filters
                    }
                },
                "highlight": {
                    "pre_tags": ["<span class=\"tema-highlight\">"],
                    "post_tags": ["</span>"],
                    "fields": {
                        "text": {}
                    }
                },
                "_source": source_filter
            });
            es.query(esquery, function (result) {
                if (result.hits.total == 0) return;

                var hit = result.hits.hits[0];
                var doc = {
                    "id" : doc_data["doc_id"],
                    "score": hit._score,
                    "metadata": hit._source.metadata,
                    "snippets": hit.highlight.text,
                    "maths": []
                };
                doc_data["maths"].map(function (math_elem) {
                    doc.maths.push({
                        "source" : hit._source.math[math_elem.math_id],
                        "replaces" : "math" + math_elem.math_id,
                        "highlight_xpath" : math_elem.xpath });
                });
                callback(null, doc);
            }, function(error) {
                callback(error, null);
            });
        });
    });
    async.parallel(callbacks, function(err, results) {
        if (err != null) {
            error_callback(err);
            return;
        }
        result_callback(results);
    });
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
            ' output="mws-ids"' +
            ' totalreq="no">' +
            '<mws:expr>' +
                query_str +
            '</mws:expr></mws:query>';
    var mwsquery_options = {
        hostname: config.MWS_HOST,
        port: config.MWS_PORT,
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

var simplify_mathelem = function(mws_id) {
    var simplified_arr = mws_id.split("#");
    return simplified_arr[simplified_arr.length - 1];
}

var schema_query =
function(exprs, depth, cutoffMode, limit,
        result_callback, error_callback) {

    if (DEBUG) util.log("Got " + exprs.length + " exprs");

    if (exprs.length == 0) {
        var reply = {
            'total' : 0,
            'schemata' : []
        };
        result_callback(reply);
        return;
    }

    var schema_query_data =
        '<mws:query' +
            ' output="json" ' + 
            ' cutoff_mode="' + cutoffMode + '"' +
            ' schema_depth="' + depth + '"' +
            ' answsize="' + limit + '">';
    for (var i in exprs) {
        expr = exprs[i];
        schema_query_data += 
            '<mws:expr>' +
                getCMML(expr) +
            '</mws:expr>';
    }
    schema_query_data += '</mws:query>';

    var schema_query_options = {
        hostname: config.SCHEMA_HOST,
        port: config.SCHEMA_PORT,
        path: '/',
        method: 'POST',
        headers: {
            'Content-Type': 'application/xml',
            'Content-Length': Buffer.byteLength(schema_query_data, 'utf8')
        }
    };

    var req = http.request(schema_query_options, function(response) {
        if (response.statusCode == 200) {
            var raw_reply = '';
            response.on('data', function (chunk) {
                raw_reply += chunk;
            });
            response.on('end', function () {
                var json_reply = JSON.parse(raw_reply);
                
                var result = {};
                result['total'] = json_reply['total'];
                result['schemata'] = [];

                get_sch_result(json_reply['schemata'], result['schemata'],
                    exprs, urls);
                if (DEBUG) util.log("Finished schematization");
                result_callback(result);
            });
        } else {
            var raw_data = '';
            response.on('data', function (chunk) {
                raw_data += chunk;
            });
            response.on('end', function () {
                var json_reply = {
                    status_code : response.statusCode,
                    data : raw_reply
                };
                error_callback(json_reply);
            });
        }
    });

    req.on('error', function(error) {
        error.status_code = 500;
        error_callback(error);
    });

    req.write(schema_query_data);
    req.end();
};

var get_sch_result = function(sch_reply, sch_result, exprs, urls) {
    sch_reply.map(function(s) {
        var sch_result_elem = {};
        sch_result_elem['coverage'] = s['coverage'];

        sch_result_elem['subst'] = [];
        s['subst'].map(function(subst) {
            sch_result_elem['subst'].push(subst);
        });

        // choose first formula as representative for schematizing
        sch_result_elem['title'] = s['formulae'][0];

        sch_result.push(sch_result_elem);
    });
};

function getCMML(expr) {
    var CMML_REGEX =
        /<annotation-xml[^>]*Content[^>]*>(.*?)<\/annotation-xml>/g;
    var match = CMML_REGEX.exec(expr);
    if (match == null) return "";
    else return match[1];
}


