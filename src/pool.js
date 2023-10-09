const { Pool } = require('pg')
const postgres = {};
let dotenv = require('dotenv').config()
let debug_mode = false;

const defaultConfig = {
    user: process.env.pgUsername,
    host:  process.env.pgHost,
    database: process.env.pgDatabase,
    password: process.env.pgPassword,
    port: process.env.pgPort
}


postgres.connectTo = function (cfg = defaultConfig) {
    let pool = new Pool(cfg);
    debug_mode = cfg.debug;

    pool.on('error', (err, client) => {
        console.error('Unexpected error on idle client', err);
        process.exit(-1);
    });

    pool.setDebug = function (debug) {
        debug_mode = debug;
    }

    pool.pq = function (query, params, return_first) {
        return new Promise((resolve, reject) => {
            pool.query(query, params, (err, data) => {
                if ( debug_mode ) {
                    console.log( query );
                }

                if ( err ) return reject(err);

                if ( return_first === true && data.rows.length > 0 ) {
                    return resolve(data.rows[0])
                }
                return resolve(data.rows);
            });
        });
    }

    return pool;
}

module.exports = postgres;