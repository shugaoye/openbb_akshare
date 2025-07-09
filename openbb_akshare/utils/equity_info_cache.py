import sqlite3
from typing import Optional, List, Dict, Any
from pathlib import Path
import pandas as pd
from datetime import date

class EquityInfoCache:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._ensure_db_exists()

    def _ensure_db_exists(self):
        """Ensure the SQLite database and table exist."""
        if not Path(self.db_path).exists():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS equity_info (
                        symbol TEXT PRIMARY KEY,
                        org_name_en TEXT,
                        main_operation_business TEXT,
                        org_cn_introduction TEXT,
                        chairman TEXT,
                        org_website TEXT,
                        reg_address_cn TEXT,
                        office_address_cn TEXT,
                        telephone TEXT,
                        postcode TEXT,
                        provincial_name TEXT,
                        staff_num INTEGER,
                        affiliate_industry TEXT,
                        operating_scope TEXT,
                        listed_date DATE,
                        org_name_cn TEXT,
                        org_short_name_cn TEXT,
                        org_short_name_en TEXT,
                        org_id TEXT,
                        established_date DATE,
                        actual_issue_vol INTEGER,
                        reg_asset REAL,
                        issue_price REAL,
                        currency TEXT
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
            df.to_sql('equity_info', conn, if_exists='replace', index=False)

    def read_dataframe(self) -> pd.DataFrame:
        """
        Read data from the SQLite database and return as a DataFrame.
        """
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT * FROM equity_info"
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
                conn.execute("DELETE FROM equity_info WHERE symbol = ?", (symbol,))
                # Insert new row
                columns = list(row.index)
                values = list(row.values)
                query = f'''
                    INSERT INTO equity_info ({', '.join(columns)})
                    VALUES ({', '.join(['?']*len(columns))})
                '''
                conn.execute(query, values)
            conn.commit()