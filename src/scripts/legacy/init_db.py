# init_db.py
"""
Script para crear las tablas en PostgreSQL
"""

import psycopg2
from src.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

def create_tables():
    """Crear tablas en PostgreSQL"""
    
    # Conectar a PostgreSQL
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    
    cursor = conn.cursor()
    
    # SQL para crear tablas
    sql = """
    CREATE TABLE IF NOT EXISTS normas (
        id SERIAL PRIMARY KEY,
        numero VARCHAR(50) UNIQUE NOT NULL,
        año INT,
        titulo TEXT,
        url VARCHAR(255),
        fecha_publicacion DATE,
        estado VARCHAR(20) DEFAULT 'pendiente',
        fecha_creacion TIMESTAMP DEFAULT NOW(),
        fecha_ultima_revision TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS chunks (
        id SERIAL PRIMARY KEY,
        norma_id INT NOT NULL REFERENCES normas(id) ON DELETE CASCADE,
        indice INT,
        texto TEXT,
        fecha_creacion TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_chunks_norma_id ON chunks(norma_id);
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        print("✅ Tablas creadas correctamente")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Creando tablas en PostgreSQL...")
    create_tables()
