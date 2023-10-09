const express = require('express')
const app = express();
let dotenv = require('dotenv').config();
const port = process.env.PORT;
const getExchangers = require('./src/controllers/getExchangers');
const { exec } = require("child_process");


app.get('/exchangers', getExchangers);

app.get('/run-import', async (req, res) => {
    try {
        await new Promise( (res, reject)=> exec('npm run parse_data', (error) => {
            if(error) reject(error);
            else res();
        }))
        res.status(200);
        res.send({ data: "Data successfully imported!" });
    } catch (e) {
        res.status(500);
        res.send({ error: "Server side error!" });
    }
})


app.listen(port, () => {
    console.log(`REST READY TO REQUEST ON PORT: ${port}`)
})