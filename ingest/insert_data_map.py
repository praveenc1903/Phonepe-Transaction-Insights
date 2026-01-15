import uuid
from utils.database import get_connection

from ingest.extract_data import (
    Agg_trans,
    Agg_user,
    Agg_insur,
    Map_trans,
    Map_users,
    Map_insur,
    Top_trans,
    Top_user,
    Top_insur
)
def bulk_insert(query, rows, label):
    conn = get_connection()
    cur = conn.cursor()
    cur.executemany(query, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {label} inserted successfully")

def insert_map_transaction(df):
    query = """
    INSERT INTO map_transaction (
        id, states, years, quarter,
        district, transaction_count, transaction_amount
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (states, years, quarter, district)
    DO NOTHING;
    """

    rows = [
        (
            str(uuid.uuid4()),
            row["States"],
            int(row["Years"]),
            int(row["Quarter"]),
            row["District"],
            int(row["Transaction_count"]),
            float(row["Transaction_amount"])
        )
        for _, row in df.iterrows()
    ]

    bulk_insert(query, rows, "Map Transaction")


def insert_map_user(df):
    query = """
    INSERT INTO map_user (
        id, states, years, quarter,
        district, registered_user, app_opens
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (states, years, quarter, district)
    DO NOTHING;
    """

    rows = [
        (
            str(uuid.uuid4()),
            row["States"],
            int(row["Years"]),
            int(row["Quarter"]),
            row["District"],
            int(row["RegisteredUser"]),
            int(row["AppOpens"])
        )
        for _, row in df.iterrows()
    ]

    bulk_insert(query, rows, "Map User")

def insert_map_insurance(df):
    query = """
    INSERT INTO map_insurance (
        id, states, years, quarter,
        district, transaction_count, transaction_amount
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (states, years, quarter, district)
    DO NOTHING;
    """

    rows = [
        (
            str(uuid.uuid4()),
            row["States"],
            int(row["Years"]),
            int(row["Quarter"]),
            row["District"],
            int(row["Transaction_count"]),
            float(row["Transaction_amount"])
        )
        for _, row in df.iterrows()
    ]

    bulk_insert(query, rows, "Map Insurance")

def main():
    insert_map_transaction(Map_trans)
    insert_map_user(Map_users)
    insert_map_insurance(Map_insur)

    print("✅ All map tables inserted successfully")


if __name__ == "__main__":
    main()
