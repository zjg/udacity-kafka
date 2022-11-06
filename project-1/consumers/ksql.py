"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# : Complete the following KSQL statements.
# : For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# : For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    line STRING,
    station_name STRING
) WITH (
    KAFKA_TOPIC='org.chicago.transit.turnstile',
    VALUE_FORMAT='avro',
    KEY='station_id'
);

CREATE TABLE turnstile_summary 
WITH (
    VALUE_FORMAT='json'
) AS
    SELECT station_id, COUNT(station_id) as count
    FROM turnstile
    GROUP BY station_id
;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
    except:
        print(resp.content)
        raise


if __name__ == "__main__":
    execute_statement()
