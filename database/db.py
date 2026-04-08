import sqlite3
import os
from pathlib import Path

from flask import g

DB_PATH = Path(
    os.environ.get(
        "SPENDLY_DB_PATH",
        str(Path(__file__).resolve().parent.parent / "expense_tracker.db"),
    )
)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_CATEGORIES = [
    "Food",
    "Transport",
    "Shopping",
    "Rent",
    "Utilities",
    "Health",
    "Entertainment",
    "Travel",
    "Education",
    "Other",
]


def get_db():
    if "db" not in g:
        connection = sqlite3.connect(DB_PATH)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON;")
        g.db = connection
    return g.db


def close_db(_error=None):
    connection = g.pop("db", None)
    if connection is not None:
        connection.close()


def init_db():
    connection = sqlite3.connect(DB_PATH)
    connection.execute("PRAGMA foreign_keys = ON;")

    schema = """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        is_default INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, name),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        category_id INTEGER NOT NULL,
        amount REAL NOT NULL CHECK (amount > 0),
        note TEXT,
        payment_method TEXT NOT NULL DEFAULT 'Card',
        spent_on TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE RESTRICT
    );

    CREATE TABLE IF NOT EXISTS budgets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        category_id INTEGER NOT NULL,
        month TEXT NOT NULL,
        amount REAL NOT NULL CHECK (amount > 0),
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, category_id, month),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_expenses_user_spent_on
        ON expenses (user_id, spent_on);
    CREATE INDEX IF NOT EXISTS idx_expenses_user_category
        ON expenses (user_id, category_id);
    CREATE INDEX IF NOT EXISTS idx_budgets_user_month
        ON budgets (user_id, month);
    """

    connection.executescript(schema)
    connection.commit()
    connection.close()


def create_default_categories(connection, user_id):
    connection.executemany(
        """
        INSERT OR IGNORE INTO categories (user_id, name, is_default)
        VALUES (?, ?, 1)
        """,
        [(user_id, category_name) for category_name in DEFAULT_CATEGORIES],
    )
    connection.commit()


def seed_db():
    init_db()
