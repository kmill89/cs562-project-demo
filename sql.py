import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

query_1 = """
WITH
  ny AS (
    SELECT cust, AVG(quant) AS "1_avg_quant"
    FROM sales
    WHERE state = 'NY'
    GROUP BY cust
  ),
  ct AS (
    SELECT cust, AVG(quant) AS "2_avg_quant"
    FROM sales
    WHERE state = 'CT'
    GROUP BY cust
  ),
  all_cust AS (
    SELECT DISTINCT cust
    FROM sales
  )
SELECT
  a.cust,
  n."1_avg_quant",
  c."2_avg_quant"
FROM all_cust a
LEFT JOIN ny n ON a.cust = n.cust
LEFT JOIN ct c ON a.cust = c.cust;
"""

query_2 = """
WITH quant_2020 AS (
    SELECT cust, quant
    FROM sales
    WHERE year = 2020
)
SELECT cust, SUM(quant) AS sum_2020
FROM quant_2020
GROUP BY cust;
"""

query_3 = """
WITH feb_orders AS (
    SELECT state
    FROM sales
    WHERE year = 2020 AND month = 2
)
SELECT state, COUNT(*) AS num_orders
FROM feb_orders
GROUP BY state
HAVING COUNT(*) > 1;
"""


def query():
    """
    Used for testing standard queries in SQL.
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    #cur.execute("SELECT * FROM sales WHERE quant > 10")
    cur.execute(query_1)

    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()
