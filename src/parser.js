const pool = require('./pool').connectTo();
const path = require("path");
const fs = require("fs");
const YAML = require('yaml');
let dotenv = require('dotenv').config()


const writeResultToDb = async (res) => {
    const offices = Array.isArray(res['exchange-offices'])
        ? res['exchange-offices'] : [ res['exchange-offices']['exchange-office'] ],
        countries = Array.isArray(res['countries'])
            ? res['countries'] : [ res['countries'].country ];
    await Promise.all(countries.map(e => pool.pq(
        `insert into countries (name, code) values ($1, $2)`,
        [e.name, e.code]
    )));

    return Promise.all(offices.map(async ({ exchanges, rates, ...office}) => {
        const exchangesToDb = (Array.isArray(exchanges)
                ? exchanges
                : exchanges?.exchange
                    ? [exchanges?.exchange]
                    : null)
            ?.map(e => ({ ...e, officeId: office.id }));

        const ratesToDb = (Array.isArray(rates)
            ? rates
            : rates?.rate
                ? [rates?.rate]
                : null)
            ?.map(e => ({ ...e, officeId: office.id }));


        await pool.pq(
            `insert into offices (id, name, country) values ($1, $2, $3)`,
            [office.id, office.name, office.country]
        );


        if(exchangesToDb)
            await Promise.all(exchangesToDb.map(({ from, to, ask, date, officeId }) => pool.pq(
                `insert into exchanges ("from", "to", ask, date, officeId) values ($1, $2, $3, $4, $5)`,
                [from, to, ask, new Date(date), officeId]
            )));

        if(ratesToDb)
            await Promise.all(ratesToDb.map(({ in: _in, out, reserve, date, from, to, officeId }) => pool.pq(
                `insert into rates ("in", out, reserve, date, "from", "to", officeId) values ($1, $2, $3, $4, $5, $6, $7)`,
                [_in, out, reserve, new Date(date), from, to, officeId]
            )));

    }))
}

const removeAll = async () => {
    await pool.pq('delete from exchanges e where e.officeId != 0');
    await pool.pq('delete from rates r where r.officeId != 0');
    await pool.pq('delete from offices o where o.id != 0');
    await pool.pq('delete from countries c where c.code != \'fg\'');
}

const parseWithConvertingToYAML = (data) => {

    /* ToDo: As presented format almost same as YAML I think the best way in this case is a just a convert file to this format
             and simple processing like as JS object.
        PS: Of course we can do a big and hard working parser, but who will do it :) ?
    * */
    let newData = '';
    const lines = data.replace(/=/g, ':')
        .split(/\r\n/g)
        .filter(e => e)
        .map(e => !/:/.test(e) ? e + ':' : e);


    let error = null, result = {};
    lines.map(e => newData+=e+'\r\n');
    console.log(newData)

    do {

        try {

            // ToDo: detecting all arrays and transforming to YAML format

            if(!error) result = YAML.parse(newData);
            if(error && error.code === 'DUPLICATE_KEY') {
                newData = newData.replace(new RegExp(`${error.toArray.trim()}`, 'g'), `-`);
                result = YAML.parse(newData);
                return result
            } else if (error && error.code !== 'DUPLICATE_KEY') {
                throw new Error('Unexpected error!');
            }
            error = null;
        } catch (e) {
            if(e && e.code === 'DUPLICATE_KEY')
                error = {
                    toArray: lines[e.linePos[0].line - 1],
                    code: e.code
                };
            else console.error(e);
        }

    } while (error);
}



const start = async () => {
        const fileDir = path.join(process.env.importDir, process.env.importFileName);

        // toDO: check if file exist
        if(!fs.existsSync(fileDir)) throw new Error('Nothing to import!');

        const dataFile = fs.readFileSync(fileDir, 'utf-8');
        await removeAll();
        const data = await parseWithConvertingToYAML(dataFile);
        await writeResultToDb(data);
}

start().catch(e => console.error('Error: ', e));