const bit = require("./bit")
const socket = require("./socket")
const init = async function(config) {

  // Cconnection pool init
  const connections = { pool: {} }

  // Bitdb Consumer Init
  console.log("# Bit Init")
  let b = (config && config.bit ? Object.assign({transform: config.transform, topics: config.topics}, config.bit) : {})
  b.connections = connections
  let manager = await bit.init(b)

  // SSE Producer Init
  console.log("# Socket Init")
  let s = (config && config.socket ? Object.assign({transform: config.transform, topics: config.topics}, config.socket) : {})
  s.connections = connections
  socket.init(s)
  return manager
}
module.exports = { init: init }
