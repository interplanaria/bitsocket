// Input: ZMQ
const zmq = require("zeromq")
const mingo = require("mingo")
const jq = require('bigjq')
const Agenda = require('agenda')
const defaults = {
  zmq: { host: "127.0.0.1", port: 28339 },
  mongo: { host: "127.0.0.1", port: 27020 }
}
/*
config := {
  zmq: {
    host: [host],
    port: [port]
  },
  mongo: {
    host: [host],
    port: [port]
  }
}
*/
var transform 
const filter = async function(event) {
  let message = event.message
  let topic = event.topic
  const encoded = event.transform.request(event.query)
  let sendpush = true
  if (encoded.q.db) {
    // if 'db' exists,
    // and it topic is included in db, 
    // send push
    if (!encoded.q.db.includes(topic)) {
      sendpush = false
    }
  }
  // if filtered out on the 'db' level, return 
  if (!sendpush) return 

  let _filter = new mingo.Query(encoded.q.find)
  if (Array.isArray(message)) {
    let _filtered = message.filter(function(e) {
      return _filter.test(e)
    })
    let transformed = []
    for(let i=0; i<_filtered.length; i++) {
      let tx = _filtered[i]
      // the stored data has returned.
      // transform the stored transaction into user-facing format
      let decoded = event.transform.response(tx)
      let result
      try {
        if (encoded.r && encoded.r.f) {
          result = await jq.run(encoded.r.f, [decoded])
        } else {
          result = decoded
        }
        transformed.push(result)
      } catch (e) {
        console.log("Error", e)
      }
    }
    return transformed
  } else {
    if (_filter.test(message)) {
      let decoded = event.transform.response(message)
      try {
        if (encoded.r && encoded.r.f) {
          try {
            let result = await jq.run(encoded.r.f, [decoded])
            return result
          } catch (e) {
            console.log("#############################")
            console.log("## Error")
            console.log(e)
            console.log("## Processor:", encoded.r.f)
            console.log("## Data:", [decoded])
            console.log("#############################")
            return
          }
        } else {
          return [decoded]
        }
      } catch (e) {
        return
      }
    } else {
      return
    }
  }
};
const init = async function(config) {
  var sock = zmq.socket("sub")
  var agenda
  let concurrency = (config.concurrency && config.concurrency.mempool ? config.concurrency.mempool : 1)
  let zmqhost = ((config.zmq && config.zmq.host) ? config.zmq.host : defaults.zmq.host)
  let zmqport = ((config.zmq && config.zmq.port) ? config.zmq.port : defaults.zmq.port)
  let mongohost = ((config.mongo && config.mongo.host) ? config.mongo.host : defaults.mongo.host)
  let mongoport = ((config.mongo && config.mongo.port) ? config.mongo.port : defaults.mongo.port)
  let connections = config.connections
  transform = config.transform
  agenda = new Agenda({
    db: {address: mongohost + ":" + mongoport + "/agenda" },
    defaultLockLimit: 1000,
    maxConcurrency: 1,
  })
  agenda.define('SSE', (job, done) => {
    let data = job.attrs.data
    let topic = data.topic
    let message = data.message
    console.log("[JOB]", topic, message.substring(0,140))
    let event = JSON.parse(message)

    // for each connection in the pool
    // find the address 
    // and the filter attached to the address
    // run the filter
    // and send the SSE
    let promises = Object.keys(connections.pool).map(function(key) {
      let connection = connections.pool[key]
      let address = connection.address
      return new Promise(async function(resolve, reject) {
        /********************************************************
        *
        *   "Event"
        *   send an "message" to a connection if the address and the topic match
        *
        *   message: the event itself
        *   query: the declarative query object expressed in JSON
        *
        ********************************************************/

        // if the 'db' attribute doesn't exist, full stream
        // if 'db' is specified, check that the topic is included in the connection's db
        if (event.address === address &&
            (!connection.query.db || connection.query.db.includes(topic))
        ) {
          let res = await filter({
            topic: topic,
            message: event.data,
            transform: transform[address],
            query: connection.query
          })
          if (typeof res !== 'undefined') {
            connection.res.sseSend({
              type: topic,
              data: res,
              //query: connection.query
            })
          }
        }
        resolve()
      })
    })
    Promise.all(promises).then(function() {
      console.log("## Finished")
      done()
    })
  })
  await agenda.start()
  console.log("Job queue started")

  // ZMQ
  console.log("Connecting zmq to : " + "tcp://" + zmqhost + ":" + zmqport)
  sock.connect("tcp://" + zmqhost + ":" + zmqport)

  // subscribe to all filters from all address from all topics
  for(let address in config.topics) {
    let topics = config.topics[address]
    //let topics = conofiObject.keys(filter)
    topics.forEach(function(topic) {
      console.log("#### Subscribing to topic", topic, "for", address)
      sock.subscribe(topic)
    })
  }
  sock.on("message", function(topic, message) {
    let type = topic.toString()
    let o = message.toString()
    agenda.now("SSE", { topic: type, message: o })
  })

  return {
    sock: sock,
    agenda: agenda
  }
}
module.exports = { init: init }
