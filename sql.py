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
WITH ny AS (
    SELECT cust, prod, AVG(quant) AS "1_avg_quant"
    FROM sales
    WHERE state = 'NY'
    GROUP BY cust, prod
),
ct AS (
    SELECT cust, prod, AVG(quant) AS "2_avg_quant"
    FROM sales
    WHERE state = 'CT'
    GROUP BY cust, prod
)
SELECT *
FROM ny
NATURAL JOIN ct
WHERE "2_avg_quant" > "1_avg_quant"
ORDER BY cust, prod;
"""

query_3 = """
WITH mar AS (
    SELECT cust, state, MIN(quant) AS "1_min_quant"
    FROM sales
    WHERE month = 3
    GROUP BY cust, state
),
may AS (
    SELECT cust, state, MAX(quant) AS "2_max_quant"
    FROM sales
    WHERE month = 5
    GROUP BY cust, state
)
SELECT *
FROM mar
NATURAL JOIN may
ORDER BY cust, state;
"""

query_4 = """
WITH d10 AS (
    SELECT cust, year, COUNT(*) AS "1_count_quant"
    FROM sales
    WHERE day = 10
    GROUP BY cust, year
),
y22 AS (
    SELECT cust, year, SUM(quant) AS "2_sum_quant"
    FROM sales
    WHERE year = 2022
    GROUP BY cust, year
)
SELECT *
FROM d10
NATURAL JOIN y22
WHERE "2_sum_quant" > 1000
ORDER BY cust, year;
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
    cur.execute(query_4)

    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()
