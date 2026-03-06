#!/usr/bin/env python3
"""Test SGGG PSC connection (ODBC DSN=PSC_VIEWER). Run from DataBridge folder: python scripts/test_sggg_connection.py"""
import sys
try:
    import pyodbc
except ImportError:
    print("FAIL: pyodbc not installed. Run: pip install pyodbc")
    sys.exit(1)
try:
    conn = pyodbc.connect("DSN=PSC_VIEWER")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    conn.close()
    print("OK: SGGG PSC connection successful (DSN=PSC_VIEWER).")
except Exception as e:
    print(f"FAIL: {e}")
    sys.exit(1)
