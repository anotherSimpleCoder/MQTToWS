const mqtt = require('mqtt')
const WebSocket = require('ws')
const RxJs = require('rxjs')
const fs = require('fs')

/**
 * @typedef {Object} IConfig
 * @property {string} uri
 * @property {string} topic
 * @property {string} username
 * @property {string} password
 */

/**
 * This function is used to connect to the broker.
 * 
 * @param {string} host     The MQTT host
 * @param {string} username The username for the host
 * @param {string} password The password for the host
 * 
 * 
 * @returns {Promise<mqtt.MqttClient>}
 */
function connectClient(host, username, password) {
    const brokerUrl = `mqtts://${username}:${password}@${host}`
    const client = mqtt.connect(brokerUrl)

    return new Promise((resolve, reject)=> {
        client.on("connect", ()=> {
            console.log("Connected")
            resolve(client)
        })

        client.on("error", (err)=> {
            reject(new Error(err))
        })
    })
}

/**
 * This function fetches messages from a topic and puts the stream into an rxjs observable
 * 
 * @param {mqtt.MqttClient} client after connection
 * @param {string} topic  the topic to fetch messages from
 * 
 * @returns {Promise<Observable<string>>} the observable of the topic message stream
 */
function subscribeClient(client, topic) {
    return new Promise((resolve, reject)=> {
        const obs = new RxJs.Observable((obs)=> {
            //Subscribe to topic
            client.subscribe(topic, (err)=> {
                reject(err)
            })
    
            //Fetch message
            client.on("message", (_topic, data)=> {
                obs.next(data.toString())
            })    
        })

        resolve(obs)
    })
}

/**
 * This function starts a websocket server and does the broker stuff.
 * 
 * @param {IConfig} cfg 
 */
async function initServer(cfg){
    //Start mqtt broker client
    const mqttCli = await connectClient(cfg.uri, cfg.username, cfg.password)
    const obs = await subscribeClient(mqttCli, cfg.topic)

    //Set up WS Server
    const server = new WebSocket.Server({port: 3000})
    console.log("listening 3000....")

    server.on("connection", (s)=> {
        const pid = setInterval(()=> {
            if(s.readyState === WebSocket.OPEN) {
                obs.subscribe((data)=> {
                    s.send(data)
                })
            }
        }, 3000)
        
        s.on("close", ()=> {
            clearInterval(pid)
        })
    })
}

async function main() {
    //Look for config.json and fetch config data
    if(!fs.existsSync("config.json")) {
        throw new Error("config.json missing!")
    } 

    /**@type {IConfig} */
    let fileContent = fs.readFileSync("config.json", {encoding: "utf8"})

    await initServer(JSON.parse(fileContent))
}

if(require.main === module) {
    main()
}