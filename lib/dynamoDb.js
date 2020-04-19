var AWS = require('aws-sdk');

const util = require('util');
// Require the base SqlConnector class
const Connector = require('loopback-connector').Connector;
// Require the debug module with a pattern of loopback:connector:connectorName
const debug = require('debug')('loopback:connector:dynamodb');

function getFilterExpressionAndExpressionAttributeValues(where, partitionKey, partitionKeyValue, sortKey) {
  const expressionAttributeValues = {};
  const allExpressions = [];
  expressionAttributeValues[':partitionKey'] = partitionKeyValue;

  const whereKeys = Object.keys(where);
  for (let i = 0; i < whereKeys.length; i++) {

    const key = whereKeys[i];
    const value = where[key];

    if (key !== partitionKey && key !== sortKey) {
      allExpressions.push(`${key} = :${key}`);
      expressionAttributeValues[`:${key}`] = value;
    }

    if (key === sortKey) {
      expressionAttributeValues[':sortKey'] = where[sortKey];
    }

  }

  return {
    filterExpression: allExpressions.join(' and '),
    expressionAttributeValues
  }
}

function getUpdateExpressionAndExpressionAttributeValues(data, partitionKey, sortKey) {
  const expressionAttributeValues = {};
  const allExpressions = [];

  const dataKeys = Object.keys(data);
  for (let i = 0; i < dataKeys.length; i++) {

    const key = dataKeys[i];
    const value = data[key];

    if (key !== partitionKey && key!==sortKey) {
      allExpressions.push(`${key} = :${key}`);
      expressionAttributeValues[`:${key}`] = value;    }

   
  }

  return {
    updateExpression: `set ${allExpressions.join(', ')}`,
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

  const { filterExpression, expressionAttributeValues } = getFilterExpressionAndExpressionAttributeValues(filter.where, dynamoDbSettings.partitionKey, options.partitionKeyValue, dynamoDbSettings.sortKey);

  const keyConditionExpressions = [`${dynamoDbSettings.partitionKey} = :partitionKey`];

  if (filter.where[dynamoDbSettings.sortKey]) {
    keyConditionExpressions.push(`${dynamoDbSettings.sortKey} = :sortKey`);
  }

  const params = {
    TableName: dynamoDbSettings.tableName,
    KeyConditionExpression: keyConditionExpressions.join(' and '),
    ExpressionAttributeValues: expressionAttributeValues,
  };

  if (filterExpression) {
    params.FilterExpression = filterExpression;
  }

  this.docClient.query(params, function (err, data) {
    if (err) {
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

  const { filterExpression, expressionAttributeValues } = getFilterExpressionAndExpressionAttributeValues(where, dynamoDbSettings.partitionKey, options.partitionKeyValue);

  const params = {
    TableName: dynamoDbSettings.tableName,
    KeyConditionExpression: `${dynamoDbSettings.partitionKey} = :partitionKey`,
    ExpressionAttributeValues: expressionAttributeValues,
  };

  if (filterExpression) {
    params.FilterExpression = filterExpression;
  }

  this.docClient.query(params, function (err, data) {
    if (err) {
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
  this.docClient.put(params, function (err, data) {
    if (err) {
      callback(err)
    }
    else {
      callback(null, data.Items);
    }
  });
}

DynamoDB.prototype.update  = function (model, where, data, options, callback) {
  const modelClass = this._models[model];

  const dynamoDbSettings = modelClass.settings.dynamodb;
  const { updateExpression, expressionAttributeValues } = getUpdateExpressionAndExpressionAttributeValues(data, dynamoDbSettings.partitionKey, dynamoDbSettings.sortKey);

  const params2 = {
    TableName: dynamoDbSettings.tableName,
    Key: {
        [dynamoDbSettings.sortKey]: where[dynamoDbSettings.sortKey],
        [dynamoDbSettings.partitionKey]: options.partitionKeyValue
    },
    UpdateExpression: updateExpression,
    ExpressionAttributeValues: expressionAttributeValues,
    ReturnValues:"ALL_NEW"
  };
  this.docClient.update(params2, function (err, data) {
    if (err) {
      callback(err)
    }
    else {
      callback(null, data.Attributes);
    }
  });
}

util.inherits(DynamoDB, Connector);

exports.initialize = function initializeDataSource(dataSource, callback) {

  const s = dataSource.settings;

  s.safe = (s.safe !== false);

  dataSource.connector = new DynamoDB(s, dataSource);

  if (callback) {
    dataSource.connector.connect(callback);
  }
};
