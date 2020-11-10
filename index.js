const redis = require('redis');
const uuid = require('uuid');

let redisHost = process.env.REDIS_HOST;
let redisPort = process.env.REDIS_PORT;
let redisPass = process.env.REDIS_PASS;

let redisClient = redis.createClient({host: redisHost, port: redisPort, password: redisPass});

const thisMany = 200000;

exports.consumer = (req, res) => {

    redisClient.on('connect', function () {
        console.log('Client Connected to ' + redisHost + ':' + redisPort);
    })
    redisClient.on("error", function (err) {
        console.log("Error: " + err);
    });

    redisClient.keys('*', function (err, keys) {
        if (err) {
            console.log(err);
            res.status(500).send(err);
        } else {
            console.log("Found keys.")
            keys.forEach(function (key,i) {
                redisClient.hgetall(key,function (err, engagement){
                    console.log(engagement);
                    redisClient.del(key);
                })
            })
        }
    });

};

exports.generator = (req, res) => {

    redisClient.on('connect', function () {
        console.log('Client Connected to ' + redisHost + ':' + redisPort);
    });
    redisClient.on("error", function (err) {
        console.log("Error: " + err);
    });

    for (let i = 0; i <= thisMany; i++) {
        console.log(`\tCreating hash ${i}`);

        let deviceId = uuid.v4();
        let branchId = uuid.v4();

        redisClient.hmset(uuid.v1(), {
            'in_out': 'in',
            'deviceId': deviceId,
            'branchId': branchId,
            'timestamp': (new Date()).getTime()
        });

        redisClient.hmset(uuid.v1(), {
            'in_out': 'out',
            'deviceId': deviceId,
            'branchId': branchId,
            'timestamp': (new Date()).getTime() + Math.round(Math.random() * 900000) //some random time under 15 minutes later
        });

    }

    res.status(200).send("Successfully created " + thisMany);
};