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

def insert_aggregated_transaction(df):
    query = """
    INSERT INTO aggregated_transaction (
        id, states, years, quarter,
        transaction_type, transaction_count, transaction_amount
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (states, years, quarter, transaction_type)
    DO NOTHING;
    """

    rows = [
        (
            str(uuid.uuid4()),
            row["States"],
            int(row["Years"]),
            int(row["Quarter"]),
            row["Transaction_type"],
            int(row["Transaction_count"]),
            float(row["Transaction_amount"])
        )
        for _, row in df.iterrows()
    ]

    bulk_insert(query, rows, "Aggregated Transaction")

def insert_aggregated_user(df):
    query = """
    INSERT INTO aggregated_user (
        id, states, years, quarter,
        brands, transaction_count, percentage
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (states, years, quarter, brands)
    DO NOTHING;
    """

    rows = [
        (
            str(uuid.uuid4()),
            row["States"],
            int(row["Years"]),
            int(row["Quarter"]),
            row["Brands"],
            int(row["Transaction_count"]),
            float(row["Percentage"])
        )
        for _, row in df.iterrows()
    ]

    bulk_insert(query, rows, "Aggregated User")
def insert_aggregated_insurance(df):
    query = """
    INSERT INTO aggregated_insurance (
        id, states, years, quarter,
        insurance_type, insurance_count, insurance_amount
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (states, years, quarter, insurance_type)
    DO NOTHING;
    """

    rows = [
        (
            str(uuid.uuid4()),
            row["States"],
            int(row["Years"]),
            int(row["Quarter"]),
            row["Insurance_type"],
            int(row["Insurance_count"]),
            float(row["Insurance_amount"])
        )
        for _, row in df.iterrows()
    ]

    bulk_insert(query, rows, "Aggregated Insurance")
def main():
    # Aggregated tables
    insert_aggregated_transaction(Agg_trans)
    insert_aggregated_user(Agg_user)
    insert_aggregated_insurance(Agg_insur)

    print("✅ All aggregated tables inserted successfully")


if __name__ == "__main__":
    main()
