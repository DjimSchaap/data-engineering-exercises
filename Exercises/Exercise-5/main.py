import csv

import psycopg2
from psycopg2 import sql


def main() -> None:
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cur = conn.cursor()

    with open("webshop.sql", encoding="utf-8") as script:
        cur.execute(script.read())

    conn.commit()

    for table, filePath in (
        ("accounts", "data/accounts.csv"),
        ("products", "data/products.csv"),
    ):
        with open(filePath, encoding="utf-8") as csvFile:
            cur.copy_expert(
                sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)").format(
                    sql.Identifier(table)
                ),
                csvFile,
            )

    with open("data/transactions.csv", newline="", encoding="utf-8") as transactionsFile:
        transactions = csv.DictReader(transactionsFile, skipinitialspace=True)
        cur.executemany(
            """
            INSERT INTO transactions (
                transaction_id,
                transaction_date,
                product_id,
                quantity,
                account_id
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            [
                (
                    transaction["transaction_id"],
                    transaction["transaction_date"],
                    transaction["product_id"],
                    transaction["quantity"],
                    transaction["account_id"],
                )
                for transaction in transactions
            ],
        )

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
