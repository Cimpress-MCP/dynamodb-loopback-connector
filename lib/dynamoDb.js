var AWS = require('aws-sdk');

const util = require('util');
// Require the base SqlConnector class
const Connector = require('loopback-connector').Connector;
// Require the debug module with a pattern of loopback:connector:connectorName
const debug = require('debug')('loopback:connector:dynamodb');

function getFilterExpressionAndExpressionAttributeValues(where, partitionKey) {
  const expressionAttributeValues = {};
  const allExpressions = [];

  const whereKeys = Object.keys(where);
  for(let i = 0; i < whereKeys.length; i++) {

    const key = whereKeys[i];
    const value = where[key];

    if( key !== partitionKey) {
      allExpressions.push(`${key} = :${key}`);
      expressionAttributeValues[`:${key}`] = value;
    }
    else {
      expressionAttributeValues[':partitionKey'] = value;
    }
  }

  return {
    filterExpression: allExpressions.join(' and '),
    expressionAttributeValues
  }
}

function DynamoDB(settings, dataSource) {

  Connector.call(this, 'dynamoDb', settings);

  if (!AWS) {
    throw new Error("AWS SDK not installed. Please run npm install aws-sdk");
  }

  this.debug = settings.debug || debug.enabled;

  this.dataSource = dataSource;

  this._models = {};

}

DynamoDB.prototype.connect = function (callback) {
  const DYNAMO_ENDPOINT = this.settings.region === 'local' ? 'http://localhost:8000' : `https://dynamodb.${this.settings.region}.amazonaws.com`;
  const dynamodb = new AWS.DynamoDB({
    region: this.settings.region,
    endpoint: DYNAMO_ENDPOINT,
  });
  this.docClient = new AWS.DynamoDB.DocumentClient({ service: dynamodb });
  callback(null, this.docClient);
}

DynamoDB.prototype.all = function (model, filter, options, callback) {
  const modelClass = this._models[model];

  const dynamoDbSettings = modelClass.settings.dynamodb;

  const {filterExpression, expressionAttributeValues} = getFilterExpressionAndExpressionAttributeValues(filter.where, dynamoDbSettings.partitionKey);

  const params = {
    TableName: dynamoDbSettings.tableName,
    KeyConditionExpression: `${dynamoDbSettings.partitionKey} = :partitionKey`,
    ExpressionAttributeValues: expressionAttributeValues,
  };

  if (filterExpression) {
    params.FilterExpression = filterExpression;
  }

  this.docClient.query(params, function(err, data) {
      if(err){
          callback(err)
      }
      else {
        callback(null, data.Items);
      }
  });
}

DynamoDB.prototype.count = function (model, where, options, callback) {
  const modelClass = this._models[model];

  const dynamoDbSettings = modelClass.settings.dynamodb;

  const {filterExpression, expressionAttributeValues} = getFilterExpressionAndExpressionAttributeValues(where, dynamoDbSettings.partitionKey);

  const params = {
    TableName: dynamoDbSettings.tableName,
    KeyConditionExpression: `${dynamoDbSettings.partitionKey} = :partitionKey`,
    ExpressionAttributeValues: expressionAttributeValues,
  };

  if (filterExpression) {
    params.FilterExpression = filterExpression;
  }

  this.docClient.query(params, function(err, data) {
      if(err){
          callback(err)
      }
      else {
        callback(null, data.Items.length);
      }
  });
}

DynamoDB.prototype.create = function (model, item, callback) {
  const modelClass = this._models[model];

  const dynamoDbSettings = modelClass.settings.dynamodb;
  const params = {
      TableName: dynamoDbSettings.tableName,
      Item: item
  };
  this.docClient.put(params, function(err, data) {
    if(err){
        callback(err)
    }
    else {
      callback(null, data.Items);
    }
  });
}


util.inherits(DynamoDB, Connector);

exports.initialize = function initializeDataSource(dataSource, callback) {

  const s = dataSource.settings;

  s.safe = (s.safe !== false);

  if(s.settings) {
    s.host = s.settings.host || "localhost";
    s.port = s.settings.port || 8000;
    s.region = s.settings.region || 'local';
    s.accessKeyId = s.settings.accessKeyId || 'fakeMyKeyId';
    s.secretAccessKey = s.settings.secretAccessKey || 'fakeSecretAccessKey';
  }
  else {
    s.region = 'local';
  }
 

  dataSource.connector = new DynamoDB(s, dataSource);

  if (callback) {
    dataSource.connector.connect(callback);
  }
};
