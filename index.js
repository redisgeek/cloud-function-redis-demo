const redis = require('redis');
const uuid = require('uuid');
const mysql = require('mysql');

let redisHost = process.env.REDIS_HOST;
let redisPort = process.env.REDIS_PORT;
let redisPass = process.env.REDIS_PASS;

let mysqlHost = process.env.MYSQL_HOST;
let mysqlUser = process.env.MYSQL_USER;
let mysqlPass = process.env.MYSQL_PASS;
let mysqlDatabase = process.env.MYSQL_DB;

let thisMany = process.env.THIS_MANY;

let redisClient = redis.createClient({host: redisHost, port: redisPort, password: redisPass});

let mysqlClient = mysql.createConnection({
    host: mysqlHost,
    user: mysqlUser,
    password: mysqlPass,
    database: mysqlDatabase
});

exports.createTable = (req, res) => {
    mysqlClient.connect();

    mysqlClient.query("CREATE TABLE ENGAGED( DeviceID varchar(255), BranchId varchar(255), in_out varchar(255), timestamp varchar(255) )");

    res.status(200).send("Table Created");
}

exports.consumer = (req, res) => {

    redisClient.on('connect', function () {
        console.log('Client Connected to ' + redisHost + ':' + redisPort);
    })
    redisClient.on("error", function (err) {
        console.log("Error: " + err);
    });

    mysqlClient.connect();

    redisClient.keys('*', function (err, keys) {
        if (err) {
            console.log(err);
            res.status(500).send(err);
        } else {
            console.log("Found keys.")
            keys.forEach(function (key,i) {
                redisClient.hgetall(key,function (err, engagement){
                    console.log(engagement);
                    mysqlClient.query('INSERT INTO ENGAGED (DeviceId, BranchId, in_out, timestamp) values (' + engagement.deviceId + ',' + engagement.branchId + ',' + engagement.in_out + ',' + engagement.timestamp + ')');
                    redisClient.del(key);
                })
            })
            res.status(200).send("Ding!");
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