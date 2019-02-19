// Output: SSE
const cors = require("cors")
const express = require("express")
const ip = require("ip")
const bsv = require("bsv")
const defaults = { port: 3000 }
var app
const init = function(config) {
  app = (config.app ? config.app : express())
  let connections = config.connections
  app.use(cors())
  app.use(function (req, res, next) {
    res.sseSetup = function() {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
      })
      res.sseSend({ type: "open", data: [] })
    }
    res.sseSend = function(data) {
      res.write("data: " + JSON.stringify(data) + "\n\n")
    }
    next()
  })
  let addresses = Object.keys(config.topics)
  for(let index=0; index<addresses.length; index++) {
    let address = addresses[index]
    app.get("/s/" + address + "/:b64(*)", function(req, res) {
      try {
        console.log("req.params = ", req.params)

        // bitcoin address as fingerprint
        const privateKey = new bsv.PrivateKey()
        const fingerprint = privateKey.toAddress().toString()

        res.sseSetup()
        let json = Buffer.from(req.params.b64, "base64").toString()
        console.log("json = ", json)
        let query = JSON.parse(json)
        console.log("Query = ", query)

        res.$fingerprint = fingerprint
        connections.pool[fingerprint] = { res: res, address: address, query: query }
        console.log("## Opening connection from: " + fingerprint, address)
        console.log("## Pool size is now", Object.keys(connections.pool).length)
        console.log(JSON.stringify(req.headers, null, 2))
        req.on("close", function() {
          console.log("## Closing connection from: " + res.$fingerprint)
          console.log(JSON.stringify(req.headers, null, 2))
          delete connections.pool[res.$fingerprint]
          console.log(".. Pool size is now", Object.keys(connections.pool).length)
        })
      } catch (e) {
        console.log(e)
      }
    })
  }
  // if no express app was passed in, need to bootstrap.
  if (!config.app) {
    let port = (config.port ? config.port : defaults.port)
    app.listen(port , function () {
      console.log("######################################################################################")
      console.log("#")
      console.log("#  BITSOCKET: Universal Programmable Bitcoin Push Notifications Network")
      console.log("#  Pushing Bitcoin in realtime through Server Sent Events...")
      console.log("#")
      console.log(`#  API Endpoint: ${ip.address()}:${port}/s`)
      console.log("#")
      console.log("#  Learn more at https://bitsocket.org")
      console.log("#")
      console.log("######################################################################################")
    })
  }
}
module.exports = { init: init }
