import sqlite3
from typing import Optional, List, Dict, Any
from pathlib import Path
import pandas as pd
from datetime import date

class EquityCache:
    # Extract table schema into a class variable for dynamic modification
    TABLE_SCHEMA = {
        "symbol": "TEXT PRIMARY KEY",
        "org_name_en": "TEXT",
        "main_operation_business": "TEXT",
        "org_cn_introduction": "TEXT",
        "chairman": "TEXT",
        "org_website": "TEXT",
        "reg_address_cn": "TEXT",
        "office_address_cn": "TEXT",
        "telephone": "TEXT",
        "postcode": "TEXT",
        "provincial_name": "TEXT",
        "staff_num": "INTEGER",
        "affiliate_industry": "TEXT",
        "operating_scope": "TEXT",
        "listed_date": "DATE",
        "org_name_cn": "TEXT",
        "org_short_name_cn": "TEXT",
        "org_short_name_en": "TEXT",
        "org_id": "TEXT",
        "established_date": "DATE",
        "actual_issue_vol": "INTEGER",
        "reg_asset": "REAL",
        "issue_price": "REAL",
        "currency": "TEXT"
    }

    def __init__(self, db_path: str, table_name: str = "equity_info"):
        self.db_path = db_path
        self.table_name = table_name  # New table_name attribute
        self.conn = None
        self._ensure_db_exists()

    def _ensure_db_exists(self):
        """Ensure the SQLite database and table exist."""
        if not Path(self.db_path).exists():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Dynamically generate CREATE TABLE statement using TABLE_SCHEMA
                columns_definition = ", ".join([f"{col} {dtype}" for col, dtype in self.TABLE_SCHEMA.items()])
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        {columns_definition}
                    )
                ''')
                conn.commit()

    def connect(self):
        """Establish a connection to the SQLite database."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)

    def close(self):
        """Close the database connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def write_dataframe(self, df: pd.DataFrame):
        """
        Write DataFrame to the SQLite database.
        Assumes the DataFrame has columns matching the table structure.
        """
        with sqlite3.connect(self.db_path) as conn:
            df.to_sql(self.table_name, conn, if_exists='replace', index=False)

    def read_dataframe(self) -> pd.DataFrame:
        """
        Read data from the SQLite database and return as a DataFrame.
        """
        with sqlite3.connect(self.db_path) as conn:
            query = f"SELECT * FROM {self.table_name}"
            df = pd.read_sql_query(query, conn)
        return df

    def update_or_insert(self, df: pd.DataFrame):
        """
        Remove existing records with the same 'symbol' and insert new ones.
        """
        with sqlite3.connect(self.db_path) as conn:
            for _, row in df.iterrows():
                symbol = row['symbol']
                # Remove existing row with the same symbol
                conn.execute(f"DELETE FROM {self.table_name} WHERE symbol = ?", (symbol,))
                # Insert new row
                columns = list(row.index)
                values = list(row.values)
                query = f'''
                    INSERT INTO {self.table_name} ({', '.join(columns)})
                    VALUES ({', '.join(['?']*len(columns))})
                '''
                conn.execute(query, values)
            conn.commit()