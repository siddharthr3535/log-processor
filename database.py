import sqlite3
from datetime import datetime
from typing import List, Optional
import threading

class LogDatabase:
    def __init__(self, db_path: str = "logs.db"):
        self.db_path = db_path
        self.local = threading.local()  
        self._init_db()
    
    def _get_connection(self):
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn
    
    def _init_db(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT NOT NULL,
                service TEXT NOT NULL,
                message TEXT NOT NULL,
                word_count INTEGER,
                received_at TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_level ON logs(level)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_service ON logs(service)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON logs(created_at)")
        
        conn.commit()
    
    def insert_log(self, log: dict):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO logs (level, service, message, word_count, received_at, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            log['level'],
            log['service'],
            log['message'],
            log['word_count'],
            log['received_at'],
            log['processed_at']
        ))
        
        conn.commit()
        return cursor.lastrowid
    
    def insert_logs_batch(self, logs: List[dict]):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        data = [
            (log['level'], log['service'], log['message'], 
             log['word_count'], log['received_at'], log['processed_at'])
            for log in logs
        ]
        
        cursor.executemany("""
            INSERT INTO logs (level, service, message, word_count, received_at, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, data)
        
        conn.commit()
        return len(logs)
    
    def get_stats(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        total = cursor.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
        
        by_level = {}
        for row in cursor.execute("SELECT level, COUNT(*) as count FROM logs GROUP BY level"):
            by_level[row['level']] = row['count']
        
        by_service = {}
        for row in cursor.execute("SELECT service, COUNT(*) as count FROM logs GROUP BY service"):
            by_service[row['service']] = row['count']
        
        return {
            "total_logs": total,
            "by_level": by_level,
            "by_service": by_service
        }
    
    def get_logs(self, level: Optional[str] = None, limit: int = 100):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if level:
            query = "SELECT * FROM logs WHERE level = ? ORDER BY created_at DESC LIMIT ?"
            rows = cursor.execute(query, (level.upper(), limit)).fetchall()
        else:
            query = "SELECT * FROM logs ORDER BY created_at DESC LIMIT ?"
            rows = cursor.execute(query, (limit,)).fetchall()
        
        return [dict(row) for row in rows]
    
    def search_logs(self, search_term: str, limit: int = 100):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT * FROM logs 
            WHERE message LIKE ? 
            ORDER BY created_at DESC 
            LIMIT ?
        """
        rows = cursor.execute(query, (f"%{search_term}%", limit)).fetchall()
        return [dict(row) for row in rows]