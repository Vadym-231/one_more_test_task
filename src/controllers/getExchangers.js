const pool = require('../pool').connectTo();

const getExchangers = async (req, res) => {
    const sql = `
        WITH ExchangeRates AS (
          SELECT
            er.officeId,
            er."from" AS from_currency,
            er."to" AS to_currency,
            er."in" AS exchange_rate_in,
            er.out AS exchange_rate_out,
            er.date AS exchange_rate_date
          FROM rates er
          UNION ALL
          SELECT
            er.officeId,
            er."to" AS from_currency,
            er."from" AS to_currency,
            1 / er."in" AS exchange_rate_in,
            1 / er.out AS exchange_rate_out,
            er.date AS exchange_rate_date
          FROM rates er
          WHERE er."to" = 'USD'
        ),
        ExchangerProfits AS (
          SELECT
            eo.id AS exchanger_id,
            eo.name AS exchanger_name,
            eo.country AS exchanger_country,
            eo.id AS exchanger_officeId,
            SUM(
              CASE
                WHEN ec."from" = 'USD' THEN (ec.ask - (ec.ask / er.exchange_rate_in)) * er.exchange_rate_out
                ELSE ((ec.ask / er.exchange_rate_out) - ec.ask) * er.exchange_rate_in
              END
            ) AS total_profit
          FROM offices eo
          JOIN exchanges ec ON eo.id = ec.officeId
          JOIN ExchangeRates er ON eo.id = er.officeId
          WHERE ec.date >= NOW() - INTERVAL '1 MONTH'
          GROUP BY eo.id, eo.name, eo.country, eo.id
        ),
        CountryProfits AS (
          SELECT
            cp.code AS country_code,
            cp.name AS country_name,
            SUM(ep.total_profit) AS total_country_profit
          FROM ExchangerProfits ep
          JOIN countries cp ON ep.exchanger_country = cp.code
          GROUP BY cp.code, cp.name
        ),
        Top3CountryProfits AS (
          SELECT
            cp.country_code,
            cp.country_name,
            cp.total_country_profit,
            ep.exchanger_id,
            ep.exchanger_name,
            ep.exchanger_officeId
          FROM CountryProfits cp
          JOIN ExchangerProfits ep ON cp.country_code = ep.exchanger_country
          WHERE ep.exchanger_id = (
            SELECT ep2.exchanger_id
            FROM ExchangerProfits ep2
            WHERE ep2.exchanger_country = cp.country_code
            ORDER BY ep2.total_profit DESC
            LIMIT 1
          )
          ORDER BY cp.total_country_profit DESC, ep.total_profit DESC
          LIMIT 3
        )
        SELECT
          tcp.country_code,
          tcp.country_name,
          tcp.total_country_profit,
          tcp.exchanger_name,
          eo.name AS exchanger_office_name
        FROM Top3CountryProfits tcp
        JOIN offices eo ON tcp.exchanger_officeId = eo.id;
    `;
    try {
        const data = await pool.pq(sql, []);
        res.status(200);
        res.send({ data });

    } catch (e) {
        res.status(500)
        res.send({ error: "Server side error!" })
    }
}

module.exports = getExchangers;