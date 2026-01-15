import uuid
from utils.database import get_connection
import pandas as pd

from ingest.extract_data import (
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


def insert_top_transaction(df):

    query = """
    INSERT INTO top_transaction (
        id, states, years, quarter, pincode,
        transaction_count, transaction_amount
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (states, years, quarter, pincode)
    DO NOTHING;
    """

    rows = [
        (
            str(uuid.uuid4()),
            row["States"],
            int(row["Years"]),
            int(row["Quarter"]),
            row["Pincodes"],          # <-- MUST MATCH COLUMN NAME
            int(row["Transaction_count"]),
            float(row["Transaction_amount"])
        )
        for _, row in df.iterrows()
    ]

    bulk_insert(query, rows, "Top Transaction")



def insert_top_user(df):
    insert_query = """
    INSERT INTO top_user (
        id, states, years, quarter, pincode, registered_user
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (states, years, quarter, pincode)
    DO NOTHING;
    """


    rows = [
        (
            str(uuid.uuid4()),
            row["States"],
            int(row["Years"]),
            int(row["Quarter"]),
            row["Pincodes"],
            int(row["RegisteredUser"])
        )
        for _, row in df.iterrows()
    ]

    bulk_insert(insert_query, rows, "Top User")

def insert_top_insurance(df):
    insert_query = """
    INSERT INTO top_insurance (
        id, states, years, quarter, pincode,
        transaction_count, transaction_amount
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (states, years, quarter, pincode)
    DO NOTHING;
    """

    rows = []

    for _, row in df.iterrows():
        if pd.isna(row["Pincodes"]) or row["Pincodes"] == "":
            continue

        rows.append((
            str(uuid.uuid4()),
            row["States"],
            int(row["Years"]),
            int(row["Quarter"]),
            row["Pincodes"],
            int(row["Transaction_count"]),
            float(row["Transaction_amount"])
        ))

    bulk_insert(insert_query, rows, "Top Insurance")

def main():
    #insert_top_transaction(Top_trans)
    #insert_top_user(Top_user)
    insert_top_insurance(Top_insur)

    print("✅ All TOP tables inserted successfully")

    
if __name__ == "__main__":
    main()