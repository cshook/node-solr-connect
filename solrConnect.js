var http = require('http');
var externalCallback = null;

var solrConnect = function(settings, responseHandler, callbackFunction){
    this.settings = settings;
    this.responseHandler = responseHandler;
    if (callbackFunction != null){
        externalCallback = callbackFunction;
    }
};

solrConnect.prototype = {
    constructor:solrConnect,

    // Function to initialize solrConnect Connection Object
    createConnection:function (method, settings, contentLength){
        objConnection = {};
        if (method == "GET") {
            var path = settings.solrCore.concat(settings.solrDataPath);
        } else {
            var path = settings.solrCore.concat(settings.solrUpdatePath);
            objConnection.headers = {
                'Content-Type': 'application/json',
                'Content-Length': contentLength
            };
        }
        objConnection.host = settings.serverAddress;
        objConnection.port = settings.solrPort;
        objConnection.path = path;
        objConnection.method = method;
        objConnection.coreName = settings.solrCore;
        return objConnection;
    },

    // Function to make request to Solr
    postRequest:function(connectionParams, docString, parseResponse){
        responseHandler = this.responseHandler;
        callBack = this.returnResponse;
        var sendRequest = http.request(connectionParams, function(response){
            response.setEncoding('utf-8');
            var responseString = '';


            response.on('data', function(data){
                responseString += data;
            });

            response.on('end', function(){
                callBack(responseString, responseHandler, parseResponse);
            });

            response.on('error', function(err){
                callBack(err, responseHandler, parseResponse);
            })
        });
        sendRequest.write(docString);
        sendRequest.end();
    },

    // Function to build request to update or create new document in solr
    updateCreate:function (docString){
        connectionParams = new this.createConnection('POST', this.settings, docString.length);
        this.postRequest(connectionParams, docString, 'full_response');
    },

    // Function to delete document in solr by unique document ID
    deletById:function (docID){
        docString = JSON.stringify({"delete": {"id":docID}});
        connectionParams = new this.createConnection('POST', this.settings, docString.length);
        this.postRequest(connectionParams, docString, 'full_response');
    },

    // Function to delete document in solr by using a query string
    deleteByQuery:function (queryString){
        docString = JSON.stringify('{"query": {' + queryString +'}}');
        connectionParams = new this.createConnection('POST', this.settings, docString.length);
        this.postRequest(connectionParams, docString, 'full_response');
    },

    // Function to get data from solr
    getData:function (queryString, cursorMark){
        if (typeof cursorMark === 'undefined') {
            queryString = queryString.concat('&cursorMark=*');
        } else {
            queryString = queryString.concat('&cursorMark=', cursorMark);
        }
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.path.concat(queryString);
        this.postRequest(connectionParams, '', 'data_only');
    },

    // Function to get facets from solr
    getFacets:function (queryString){
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.path.concat(queryString);
        this.postRequest(connectionParams, '', 'facets_only');
    },

    // Function to get data & facets from solr
    getDataFacets:function (queryString){
        if (typeof cursorMark === 'undefined') {
            queryString = queryString.concat('&cursorMark=*');
        } else {
            queryString = queryString.concat('&cursorMark=', cursorMark);
        }
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.path.concat(queryString);
        this.postRequest(connectionParams, '', 'data_facets');
    },

    // Function to get record count from solr
    getRecordCount:function (queryString){
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.path.concat(queryString);
        this.postRequest(connectionParams, '', 'record_count');
    },

    // Function to get field list from schema
    getFieldList:function (){
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.coreName.concat('/schema/fields');
        this.postRequest(connectionParams, '', 'data_only');
    },

    // Function to return data from solr back to requesting function
    returnResponse:function (objResponse, responseHandler, parseResponse){
        objResponse = JSON.parse(objResponse);

        // Add cursor mark for pagination to response since it lives outside of response by defualt - this allows us to return just the data when making a data only
        if (typeof objResponse.nextCursorMark !== 'undefined'){
            objResponse.response.nextCursorMark = objResponse.nextCursorMark
        }

        if (responseHandler != null){
        responseHandler.setHeader('Content-Type', 'application/json');
        }
        switch (parseResponse) {

            case 'data_only':
                if (responseHandler == null){
                    externalCallback(objResponse.response);
                } else {
                responseHandler.send(objResponse.response);
                }
            break;

            case 'full_response':
                if (responseHandler == null){
                    externalCallback(objResponse);
                } else {
                responseHandler.send(objResponse);
                }
            break;

            case 'facets_only':
                if (responseHandler == null){
                    externalCallback(objResponse.facet_counts.facet_fields);
                } else {
                responseHandler.send(objResponse.facet_counts.facet_fields);
                }
            break;

            case 'data_facets':
                tmpResponse = {data:{},facet_counts:{}};
                tmpResponse.data = objResponse.response;
                tmpResponse.facet_counts = objResponse.facet_counts.facet_fields;
                if (responseHandler == null){
                    externalCallback(tmpResponse);
                } else {
                responseHandler.send(tmpResponse);
                }
            break;

            case 'record_count':
                if (responseHandler == null){
                    externalCallback(objResponse.response.numFound);
                } else {
                    responseHandler.send(objResponse.response.numFound);
                }
            break;

            default:
                if (responseHandler == null){
                    externalCallback(objResponse);
                } else {
                responseHandler.send(objResponse);
                }
            break;
        }
    },


}

module.exports = solrConnect;