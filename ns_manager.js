'use strict'

const PACKAGE = require('./package.json')
const VERSION = PACKAGE.version

const STAGE = process.env.STAGE || 'test'
const LOCAL = process.env.LOCAL || false
const DYNAMO_NS = `pg_${STAGE}-nsTable`
const SECRETS = require('./secrets')
const AWS_ACCESS_KEY_ID = SECRETS.get('SWARM_AWS_ACCESS_KEY_ID')
const AWS_SECRET_ACCESS_KEY = SECRETS.get('SWARM_AWS_SECRET_ACCESS_KEY')
if (AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY) {
  process.env['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
  process.env['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY
}

const AWS = require('aws-sdk')
AWS.config.update({region:'us-east-1', correctClockSkew: true})
const SQS = new AWS.SQS()

let PARAMS = {}
let IOT

async function configAWS() {
  IOT = new AWS.IotData({endpoint: `${PARAMS.IOT_ENDPOINT_HOST}`})
}

console.log(`Worker Manager Version: ${VERSION} :: Stage: ${STAGE}`)
console.log(`ENV: ${JSON.stringify(process.env)}`)

// Logger intercept for easy logging in the future.
const LOGGER = {
  info: (msg, userId = 'unset') => {
    if (userId !== 'unset') {
      console.log(`NS: [${userId}] ${msg}`)
      let notificationTopic = `${STAGE}/frontend/${userId}`
      mqttSend(notificationTopic, {notification: {type: 'success', msg: msg}})
    } else {
      console.log(`NS: ${msg}`)
    }
  },
  error: (msg, userId = 'unset') => {
    if (userId !== 'unset') {
      console.error(`NS: [${userId}] ${msg}`)
      let notificationTopic = `${STAGE}/frontend/${userId}`
      mqttSend(notificationTopic, {notification: {type: 'error', msg: msg}})
    } else {
      console.error(`NS: ${msg}`)
    }
  },
  debug: (msg, userId = 'unset') => {
    if (userId !== 'unset') {
      console.log(`NS: [${userId}] ${msg}`)
    } else {
      console.log(`NS: ${msg}`)
    }
  }
}

// DB Vars and Methods
var docClient = new AWS.DynamoDB.DocumentClient()

async function getDbNodeServers(fullMsg) {
  let returnValue = {}
  let params = {
    TableName: DYNAMO_NS,
    KeyConditionExpression: "id = :id",
    ExpressionAttributeValues: {
      ":id": fullMsg.id
    }
  }
  try {
    let data = await docClient.query(params).promise()
    if (data.Count > 0) {
      for (let ns of data.Items) {
        returnValue[ns.profileNum] = ns
      }
    }
  } catch (err) {
    LOGGER.error(`getDbNodeServers: ${JSON.stringify(err, null, 2)}`, fullMsg.userId)
  }
  return returnValue
}

async function getDbNodeServer(fullMsg) {
  let returnValue = false
  let params = {
    TableName: DYNAMO_NS,
    KeyConditionExpression: "id = :id and profileNum = :profileNum",
    ExpressionAttributeValues: {
      ":id": fullMsg.id,
      ":profileNum": fullMsg.profileNum
    }
  }
  try {
    let data = await docClient.query(params).promise()
    if (data.Count > 0) {
      returnValue = data.Items[0]
    }
  } catch (err) {
    LOGGER.error(`getDbNodeServer: ${JSON.stringify(err, null, 2)}`, fullMsg.userId)
  }
  return returnValue
}

async function addnode(cmd, fullMsg) {
  sendToISY(cmd, fullMsg)
  let ns = await getDbNodeServer(fullMsg)
  if (ns && ns.nodes.hasOwnProperty(fullMsg[cmd].address)) {
    if (fullMsg[cmd].nodedefid !== ns.nodes[fullMsg[cmd].address].nodedefid) {
      LOGGER.debug(`Detected nodedef change for ${ns.name}(${ns.profileNum}) ${fullMsg[cmd].address} :: ${fullMsg.addnode.nodedefid}`, fullMsg.userId)
      fullMsg.updatenode = fullMsg.addnode
      delete fullMsg.addnode
      sendToISY('updatenode', fullMsg)
    }
  }
}

async function resultaddnode(cmd, fullMsg) {
  if (fullMsg.result.success || fullMsg.result.statusCode === 400) {
    let update = {}
    fullMsg.addnode.isPrimary = fullMsg.addnode.address === fullMsg.addnode.primary ? true : false
    fullMsg.addnode.timeAdded = +Date.now()
    fullMsg.addnode.timeStarted = 0
    let params = {
      TableName: DYNAMO_NS,
      Key: {
        "id": fullMsg.id,
        "profileNum": fullMsg.profileNum
      },
      UpdateExpression: `set nodes.#name = :node`,
      ExpressionAttributeNames: { "#name": fullMsg.addnode.address },
      ExpressionAttributeValues: {
        ":node": fullMsg.addnode
      },
      // ConditionExpression: "attribute_not_exists(nodes.#name)",
      ReturnValues: 'ALL_NEW'
    }
    try {
      let data = await docClient.update(params).promise()
      if (data.hasOwnProperty('Attributes')) {
        update = data.Attributes
        if (fullMsg.result.statusCode === 400) {
          LOGGER.debug(`addNode: Node already existed in ISY. Updated database. ${update.name}(${fullMsg.profileNum}) - ${fullMsg.addnode.name}`, fullMsg.userId)
        } else {
          LOGGER.debug(`addNode: Added Node ${update.name}(${fullMsg.profileNum}) - ${fullMsg.addnode.name}`, fullMsg.userId)
        }
        mqttSend(`${STAGE}/ns/${update.worker}`, {result: {addnode: { success: true, address: fullMsg.addnode.address }}})
        await mqttSend(`${STAGE}/ns/${update.worker}`, {config: update})
        let frontUpdate = {}
        frontUpdate[fullMsg.profileNum] = update
        mqttSend(`${STAGE}/frontend/${fullMsg.userId}`, { id: fullMsg.id, nsUpdate: frontUpdate} )
      }
    } catch (err) {
      LOGGER.error(`addNode: ${err}`, fullMsg.userId)
    }
  } else {
    if (verifyProps(fullMsg.result.json, ['RestResponse.reason.code'])) {
      LOGGER.debug(`${JSON.stringify(fullMsg)}`, fullMsg.userId)
    }
    LOGGER.error(`Couldn't add node to ISY. StatusCode ${fullMsg.result.statusCode}`, fullMsg.userId)
  }
}

async function resultremovenode(cmd, fullMsg) {
 if (fullMsg.result.success || fullMsg.result.statusCode === 403) {
    let update = {}
    let params = {
      TableName: DYNAMO_NS,
      Key: {
        "id": fullMsg.id,
        "profileNum": fullMsg.profileNum
      },
      UpdateExpression: `REMOVE nodes.#name`,
      ExpressionAttributeNames: { "#name": fullMsg.removenode.address },
      ReturnValues: 'ALL_NEW'
    }
    try {
      let data = await docClient.update(params).promise()
      if (data.hasOwnProperty('Attributes')) {
        update = data.Attributes
        LOGGER.debug(`removenode: Removed Node (${fullMsg.profileNum})${fullMsg.removenode.address} JSON: ${JSON.stringify(update)}`, fullMsg.userId)
        mqttSend(`${STAGE}/ns/${update.worker}`, {config: update})
        let frontUpdate = {}
        frontUpdate[fullMsg.profileNum] = update
        mqttSend(`${STAGE}/frontend/${fullMsg.userId}`, { id: fullMsg.id, nsUpdate: frontUpdate} )
        LOGGER.info('Removed node successfully.', fullMsg.userId)
      }
    } catch (err) {
      LOGGER.error(`addNode: ${err}`, fullMsg.userId)
    }
  } else {
    LOGGER.error(`Couldn't add node to ISY. StatusCode ${fullMsg.result.statusCode}`, fullMsg.userId)
  }
}

async function resultstatus(cmd, fullMsg) {
  if (fullMsg.result.success) {
    let update = {}
    let params = {
      TableName: DYNAMO_NS,
      Key: {
        "id": fullMsg.id,
        "profileNum": fullMsg.profileNum
      },
      UpdateExpression: `SET nodes.#name.drivers.#driver.#value = :value`,
      ExpressionAttributeNames: {
        "#name": fullMsg.status.address,
        "#driver": fullMsg.status.driver,
        "#value": 'value'
      },
      ExpressionAttributeValues: {
        ":value": fullMsg.status.value
      },
      ReturnValues: 'ALL_NEW'
    }
    try {
      let data = await docClient.update(params).promise()
      if (data.hasOwnProperty('Attributes')) {
        update = data.Attributes
        LOGGER.debug(`status: ${update.name}(${update.profileNum}) :: ${fullMsg.status.address}: ${fullMsg.status.driver} - ${fullMsg.status.value} - ${fullMsg.status.uom}`, fullMsg.userId)
        mqttSend(`${STAGE}/ns/${update.worker}`, {config: update})
        let frontUpdate = {}
        frontUpdate[fullMsg.profileNum] = update
        mqttSend(`${STAGE}/frontend/${fullMsg.userId}`, { id: fullMsg.id, nsUpdate: frontUpdate} )
      }
    } catch (err) {
      LOGGER.error(`status: ${err} JSON: ${JSON.stringify(fullMsg)}`, fullMsg.userId)
    }
  } else {
    LOGGER.error(`Couldn't update driver to ISY. StatusCode ${fullMsg.result.statusCode}`, fullMsg.userId)
  }
}

async function resultBatch(cmd, fullMsg) {
  let update
  for (let key in fullMsg.result.batch) {
    if (!['addnode', 'removenode', 'status'].includes(key)) { continue }
    let command = checkCommand(key)
    let type = fullMsg.result.batch[key]
    if (Array.isArray(type) && type.length) {
      LOGGER.debug(`Processing results for batch ${key}`, fullMsg.userId)
      update = await command.batch(key, fullMsg)
    }
  }
  if (update) {
    mqttSend(`${STAGE}/ns/${update.worker}`, {config: update})
    let frontUpdate = { [fullMsg.profileNum]: update }
    mqttSend(`${STAGE}/frontend/${fullMsg.userId}`, { id: fullMsg.id, nsUpdate: frontUpdate} )
  }
}

async function resultBatchaddnode(cmd, fullMsg) {
  try {
    let updateExpression, update
    let expressionValues = {}
    let results = fullMsg.result.batch[cmd]
    for (let result of results) {
      if (result.success || result.statusCode === 400) {
        if (result.statusCode === 400) {
          LOGGER.debug(`addNode: Node already existed in ISY. Updating database. ${fullMsg.id}(${fullMsg.profileNum}) - ${result.name}`, fullMsg.userId)
        } else {
          LOGGER.debug(`addNode: Added Node ${fullMsg.id}(${fullMsg.profileNum}) - ${result.name}`, fullMsg.userId)
        }
        let node = result
        node.isPrimary = node.address === node.primary ? true : false
        node.timeAdded = +Date.now()
        node.timeStarted = 0
        if (!updateExpression) {
          updateExpression = `SET nodes.${node.address} = :${node.address}`
        } else {
          updateExpression += `, nodes.${node.address} = :${node.address}`
        }
        expressionValues[`:${node.address}`] = node
      }
    }
    if (updateExpression) {
      let params = {
        TableName: DYNAMO_NS,
        Key: {
          "id": fullMsg.id,
          "profileNum": fullMsg.profileNum
        },
        UpdateExpression: updateExpression,
        ExpressionAttributeValues: expressionValues,
        ReturnValues: 'ALL_NEW'
      }
      let data = await docClient.update(params).promise()
      if (data.hasOwnProperty('Attributes')) {
        update = data.Attributes
        LOGGER.debug(`addnode: ${update.name}(${update.profileNum}) :: batch node add`, fullMsg.userId)
        mqttSend(`${STAGE}/ns/${update.worker}`, {result: { 'addnode': results }})
        return update
      }
    }
  } catch (err) {
    LOGGER.error(`status: ${err.stack} JSON: ${JSON.stringify(fullMsg)}`, fullMsg.userId)
  }
}

async function resultBatchremovenode(cmd, fullMsg) {}

async function resultBatchstatus(cmd, fullMsg) {
  let updateExpression, update
  let expressionValues = {}
  let results = fullMsg.result.batch[cmd]
  let updatingDrivers = []
  for (let result of results) {
    if (!result.success) { continue }
    let node = result
    if (updatingDrivers.includes(`${node.address}:${node.driver}`)) { continue }
    updatingDrivers.push(`${node.address}:${node.driver}`)
    if (!updateExpression) {
      updateExpression = `SET nodes.${node.address}.drivers.${node.driver}.#value = :${node.driver}`
    } else {
      updateExpression += `, nodes.${node.address}.drivers.${node.driver}.#value = :${node.driver}`
    }
    expressionValues[`:${node.driver}`] = node.value
  }
  if (updateExpression) {
    let params = {
      TableName: DYNAMO_NS,
      Key: {
        "id": fullMsg.id,
        "profileNum": fullMsg.profileNum
      },
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: {
        "#value": 'value'
      },
      ExpressionAttributeValues: expressionValues,
      ReturnValues: 'ALL_NEW'
    }
    try {
      let data = await docClient.update(params).promise()
      if (data.hasOwnProperty('Attributes')) {
        update = data.Attributes
        LOGGER.debug(`status: ${update.name}(${update.profileNum}) :: batch update`, fullMsg.userId)
        return update
      }
    } catch (err) {
      LOGGER.error(`status: ${err} JSON: ${JSON.stringify(fullMsg)}`, fullMsg.userId)
    }
  }
}

// Send config to NS and update frontend
async function config(cmd, fullMsg) {
  let nodeServers = getDbNodeServers(fullMsg)
  mqttSend(`${STAGE}/frontend/${fullMsg.userId}`, { getNodeServers: nodeServers })
  mqttSend(fullMsg.topic, { config: nodeServers[fullMsg.profileNum] })
}

// Only update the frontend
async function update(cmd, fullMsg) {
  let nodeServers = await getDbNodeServers(fullMsg)
  mqttSend(`${STAGE}/frontend/${fullMsg.userId}`, { getNodeServers: nodeServers })
}

// NodeServer only functions
async function connected(cmd, fullMsg) {
  let params = {
    TableName: DYNAMO_NS,
    Key: {
      "id": fullMsg.id,
      "profileNum": fullMsg.profileNum
    },
    UpdateExpression: `set #connected = :connected, timeStarted = :timeStarted, firstRun = :firstRun`,
    ExpressionAttributeNames: { "#connected": 'connected' },
    ExpressionAttributeValues: {
      ":connected": fullMsg.connected,
      ":timeStarted": fullMsg.connected ? +Date.now() : 0,
      ":firstRun": false
    },
    ConditionExpression: 'attribute_exists(id)',
    ReturnValues: 'ALL_NEW'
  }
  try {
    let data = await docClient.update(params).promise()
    if (data.hasOwnProperty('Attributes')) {
      update = data.Attributes
      LOGGER.debug(`connected: Updated Connected state for (${update.profileNum}) ${update.name} to ${update.connected}`, fullMsg.userId)
      let controller = Object.values(update.nodes).filter(node => node['isController'] === true)[0]
      if (controller) {
        sendToISY('status', {
          status: {
            address: controller.address,
            driver: 'ST',
            value: fullMsg.connected ? '1' : '0',
            uom: '2'
          },
          id: fullMsg.id,
          profileNum: fullMsg.profileNum,
          userId: fullMsg.userId
        })
      }
      mqttSend(`${STAGE}/frontend/${fullMsg.userId}`, { id: fullMsg.id, nsUpdate: {[fullMsg.profileNum]: update}} )
    }
  } catch (err) {
    LOGGER.error(`${err.stack}`)
    if (!err.code === "ConditionalCheckFailedException") {
      LOGGER.error(`connected: ${err}`, fullMsg.userId)
    }
  }
}

async function updateDatabase(cmd, fullMsg) {
  delete fullMsg[cmd].isy
  delete fullMsg[cmd].profileNum
  let params = {
    TableName: DYNAMO_NS,
    Key: {
      "id": fullMsg.id,
      "profileNum": fullMsg.profileNum
    },
    UpdateExpression: `set #name = :value`,
    ExpressionAttributeNames: { "#name": checkCommand(cmd).attrName },
    ExpressionAttributeValues: { ":value": fullMsg[cmd] },
    ConditionExpression: 'attribute_exists(id)',
    ReturnValues: 'ALL_NEW'
  }
  try {
    let data = await docClient.update(params).promise()
    if (data.hasOwnProperty('Attributes')) {
      update = data.Attributes
      LOGGER.debug(`${cmd} updated for (${update.profileNum}) ${update.name}`, fullMsg.userId)
      mqttSend(`${STAGE}/ns/${update.worker}`, { config: update })
      mqttSend(`${STAGE}/frontend/${fullMsg.userId}`, { id: fullMsg.id, nsUpdate: {[fullMsg.profileNum]: update}} )
    }
  } catch (err) {
    LOGGER.error(`${err.stack}`)
    if (!err.code === "ConditionalCheckFailedException") {
      LOGGER.error(`${cmd}: ${err}`, fullMsg.userId)
    }
  }
}

async function polls(cmd, fullMsg) {
  delete fullMsg[cmd].isy
  delete fullMsg[cmd].profileNum
  let params = {
    TableName: DYNAMO_NS,
    Key: {
      "id": fullMsg.id,
      "profileNum": fullMsg.profileNum
    },
    UpdateExpression: `set #name = :value, #name2 = :value2`,
    ExpressionAttributeNames: { '#name': 'shortPoll', '#name2': 'longPoll' },
    ExpressionAttributeValues: {
      ":value": fullMsg[cmd].shortPoll,
      ":value2": fullMsg[cmd].longPoll
    },
    ConditionExpression: 'attribute_exists(id)',
    ReturnValues: 'ALL_NEW'
  }
  try {
    let data = await docClient.update(params).promise()
    if (data.hasOwnProperty('Attributes')) {
      update = data.Attributes
      LOGGER.debug(`${cmd} updated for (${update.profileNum}) ${update.name}`, fullMsg.userId)
      let frontUpdate = {}
      frontUpdate[fullMsg.profileNum] = update
      mqttSend(`${STAGE}/ns/${update.worker}`, {[cmd]: fullMsg[cmd]})
      mqttSend(`${STAGE}/frontend/${fullMsg.userId}`, { id: fullMsg.id, nsUpdate: frontUpdate} )
    }
  } catch (err) {
    LOGGER.error(`${err.stack}`)
    if (!err.code === "ConditionalCheckFailedException") {
      LOGGER.error(`${cmd}: ${err}`, fullMsg.userId)
    }
  }
}

// Send Message to ISY Handler Lambda
async function sendToISY (cmd, fullMsg) {
  let payload = Object.assign(fullMsg, { topic: `${STAGE}/ns`})
  mqttSend(`${STAGE}/isy`, payload)
}

// MQTT Methods
async function mqttSend(topic, message, qos = 0) {
  const payload = JSON.stringify(message)
  const iotMessage = {
    topic: topic,
    payload: payload,
    qos: qos
  }
  return IOT.publish(iotMessage).promise()
}

// API
const checkCommand = (type) => apiSwitch[type] || null

const apiSwitch = {
  addnode: {
    props: [],
    func: addnode,
    result: resultaddnode,
    batch: resultBatchaddnode,
    type: 'ns'
  },
  removenode: {
    props: [],
    func: sendToISY,
    result: resultremovenode,
    batch: resultBatchremovenode,
    type: 'ns'
  },
  status: {
    props: [],
    func: sendToISY,
    result: resultstatus,
    batch: resultBatchstatus,
    type: 'ns'
  },
  command: {
    props: [],
    func: sendToISY
  },
  batch: {
    props: [],
    func: sendToISY,
    result: resultBatch,
    type: 'ns'
  },
  config: {
    props: [],
    func: config,
    type: 'ns'
  },
  update: {
    props: [],
    func: update,
    type: 'frontend'
  },
  connected: {
    props: [],
    func: connected,
    type: 'frontend'
  },
  customparams: {
    props: [],
    func: updateDatabase,
    type: 'ns',
    attrName: 'customParams',
  },
  customdata: {
    props: [],
    func: updateDatabase,
    type: 'ns',
    attrName: 'customData',
  },
  notices: {
    props: [],
    func: updateDatabase,
    type: 'ns',
    attrName: 'notices',
  },
  polls: {
    props: ['shortPoll', 'longPoll'],
    func: polls,
    type: 'frontend',
  }
}

const propExists = (obj, path) => {
  return !!path.split(".").reduce((obj, prop) => {
      return obj && obj[prop] ? obj[prop] : undefined;
  }, obj)
}

const verifyProps = (message, props) => {
  let confirm = {
    valid: true,
    missing: null
  }
  for (let prop of props) {
    if (!propExists(message, prop)) {
      confirm.valid = false
      confirm.missing = prop
      break
    }
  }
  return confirm
}

// Helper methods
const timeout = ms => new Promise(run => setTimeout(run, ms))

async function processMessage(message) {
  let props = verifyProps(message, ['userId', 'topic', 'id'])
  if (!props.valid) {
    return LOGGER.error(`Request missing required property: ${props.missing} :: ${JSON.stringify(message)}`)
  }
  if (message.hasOwnProperty('result')) {
    for (let key in message.result) {
      if (['topic', 'userId', 'userId', 'profileNum'].includes(key)) { continue }
      try {
        let command = checkCommand(key)
        if (!command) { continue }
        LOGGER.debug(`Processing results for ${key}...`, message.userId)
        await command.result(key, message)
      } catch (err) {
        LOGGER.error(`${key} result error :: ${err.stack}`, message.userId)
      }
    }
  } else {
    for (let key in message) {
      if (['userId', 'profileNum', 'id', 'topic'].includes(key)) { continue }
      try {
        let command = checkCommand(key)
        if (!command) { continue }
        let props = verifyProps(message[key], apiSwitch[key].props)
        if (!props.valid) {
          return LOGGER.error(`${key} was missing ${props.missing} :: ${JSON.stringify(message)}`, message.userId)
        }
        LOGGER.debug(`Processing command ${key}...`, message.userId)
        await command.func(key, message)
      } catch (err) {
        LOGGER.error(`${key} error :: ${err.stack}`, message.userId)
      }
    }
  }
}

async function getMessages() {
  const params = {
    MaxNumberOfMessages: 10,
    QueueUrl: PARAMS.SQS_NS,
    WaitTimeSeconds: 10
  }
  let deletedParams = {
    Entries: [],
    QueueUrl: PARAMS.SQS_NS
  }
  try {
    LOGGER.info(`Getting messages...`)
    let data = await SQS.receiveMessage(params).promise()
    if (data.Messages) {
        LOGGER.info(`Got ${data.Messages.length} message(s)`)
        let tasks = []
        for (let message of data.Messages) {
          try {
            let body = JSON.parse(message.Body)
            let msg = body.msg
            LOGGER.info(`Got Message: ${JSON.stringify(msg)}`)
            tasks.push(processMessage(msg))
          } catch (err) {
            LOGGER.error(`Message not JSON: ${message.Body}`)
          }
          deletedParams.Entries.push({
              Id: message.MessageId,
              ReceiptHandle: message.ReceiptHandle
          })
        }
        let results = []
        for (let task of tasks) {
          results.push(await task)
        }
        let deleted = await SQS.deleteMessageBatch(deletedParams).promise()
        deletedParams.Entries = []
        LOGGER.info(`Deleted Messages: ${JSON.stringify(deleted)}`)
    } else {
      LOGGER.info(`No messages`)
    }
  } catch (err) {
    LOGGER.error(err.stack)
  }
}

async function getParameters(nextToken) {
  const ssm = new AWS.SSM()
  var ssmParams = {
    Path: `/pgc/${STAGE}/`,
    MaxResults: 10,
    Recursive: true,
    NextToken: nextToken,
    WithDecryption: true
  }
  let params = await ssm.getParametersByPath(ssmParams).promise()
  if (params.Parameters.length === 0) throw new Error(`Parameters not retrieved. Exiting.`)
  for (let param of params.Parameters) {
    PARAMS[param.Name.split('/').slice(-1)[0]] = param.Value
  }
  if (params.hasOwnProperty('NextToken')) {
    await getParameters(params.NextToken)
  }
}

async function startHealthCheck() {
  require('http').createServer(function(request, response) {
    if (request.url === '/health' && request.method ==='GET') {
        //AWS ELB pings this URL to make sure the instance is running smoothly
        let data = JSON.stringify({uptime: process.uptime()})
        response.writeHead(200, {'Content-Type': 'application/json'})
        response.write(data)
        response.end()
    }
  }).listen(3000)
}

async function main() {
  await getParameters()
  if (!PARAMS.SQS_NS) {
    LOGGER.error(`No Queue retrieved. Exiting.`)
    process.exit(1)
  }
  LOGGER.info(`Retrieved Parameters from AWS Parameter Store for Stage: ${STAGE}`)
  await configAWS()
  startHealthCheck()
  try {
    while (true) {
      await getMessages()
    }
  } catch(err) {
    LOGGER.error(err.stack)
    main()
  }
}

['SIGINT', 'SIGTERM'].forEach(signal => {
  process.on(signal, () => {
    LOGGER.debug('Shutdown requested. Exiting...')
    setTimeout(() => {
      process.exit()
    }, 500)
  })
})

main()