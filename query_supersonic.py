#!/usr/bin/env python3
import psycopg2
import sys

def query_supersonic():
    # Supersonic metadata database connection
    conn_str = "host=10.0.12.252 port=5439 dbname=supersonic user=supersonic password=postgres"

    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        # First, check what tables exist
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        print("Tables in supersonic database:")
        for table in tables[:20]:  # Show first 20 tables
            print(f"  - {table[0]}")

        # Look for metric table
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name ILIKE '%metric%'
        """)
        metric_tables = cursor.fetchall()
        print("\nMetric-related tables:")
        for table in metric_tables:
            print(f"  - {table[0]}")

        # If metric table exists, query it
        if metric_tables:
            metric_table = metric_tables[0][0]
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = '{metric_table}'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            print(f"\nColumns in {metric_table}:")
            for col in columns:
                print(f"  - {col[0]}: {col[1]}")

            # Query metrics with sales amount
            cursor.execute(f"""
                SELECT id, name, biz_name, model_id
                FROM {metric_table}
                WHERE name LIKE '%销售金额%' OR name LIKE '%sales_amount%' OR biz_name LIKE '%sales%'
                LIMIT 10
            """)
            metrics = cursor.fetchall()
            print(f"\nMetrics found:")
            for metric in metrics:
                print(f"  - ID: {metric[0]}, Name: {metric[1]}, BizName: {metric[2]}, ModelID: {metric[3]}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error connecting to supersonic database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    query_supersonic()