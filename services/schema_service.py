from db import db_conn


def _column_exists(cur, table_name, column_name):
    cur.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (column_name,))
    return cur.fetchone() is not None


def _index_exists(cur, table_name, index_name):
    cur.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name = %s", (index_name,))
    return cur.fetchone() is not None


def ensure_database_schema():
    conn = db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS encontristas (
              id INT NOT NULL AUTO_INCREMENT,
              ano INT NOT NULL,
              num_ecc VARCHAR(100) NULL,
              data_casamento DATE NULL,
              nome_completo_ele VARCHAR(250) NULL,
              nome_completo_ela VARCHAR(250) NULL,
              nome_usual_ele VARCHAR(120) NULL,
              nome_usual_ela VARCHAR(120) NULL,
              apelidos JSON NULL,
              telefone_ele VARCHAR(40) NULL,
              telefone_ela VARCHAR(40) NULL,
              endereco VARCHAR(255) NULL,
              casal_visitacao VARCHAR(255) NULL,
              ficha_num VARCHAR(100) NULL,
              aceitou VARCHAR(20) NULL,
              observacao VARCHAR(255) NULL,
              observacao_extra VARCHAR(255) NULL,
              PRIMARY KEY (id),
              INDEX idx_encontristas_ano (ano),
              INDEX idx_encontristas_nome (nome_usual_ele, nome_usual_ela)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS encontreiros (
              id INT NOT NULL AUTO_INCREMENT,
              ano INT NOT NULL,
              equipe VARCHAR(120) NOT NULL,
              casal_id INT NULL,
              coordenador VARCHAR(10) NOT NULL DEFAULT 'Não',
              observacao TEXT NULL,
              status VARCHAR(40) NULL,
              PRIMARY KEY (id),
              INDEX idx_encontreiros_ano (ano),
              INDEX idx_encontreiros_equipe (equipe),
              INDEX idx_encontreiros_casal_id (casal_id),
              CONSTRAINT fk_encontreiros_encontrista
                FOREIGN KEY (casal_id) REFERENCES encontristas(id)
                ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS palestras (
              id INT NOT NULL AUTO_INCREMENT,
              ano INT NOT NULL,
              palestra VARCHAR(255) NOT NULL,
              nome_ele VARCHAR(120) NOT NULL,
              nome_ela VARCHAR(120) NOT NULL,
              observacao TEXT NULL,
              status VARCHAR(40) NULL,
              PRIMARY KEY (id),
              INDEX idx_palestras_ano (ano),
              INDEX idx_palestras_palestra (palestra)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS circulos (
              id INT NOT NULL AUTO_INCREMENT,
              ano INT NOT NULL,
              nome_circulo VARCHAR(120) NULL,
              cor_circulo VARCHAR(60) NULL,
              integrantes_original TEXT NULL,
              integrantes_atual TEXT NULL,
              coord_atual_ele VARCHAR(120) NULL,
              coord_atual_ela VARCHAR(120) NULL,
              PRIMARY KEY (id),
              INDEX idx_circulos_ano (ano)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS geocoding_cache (
              id INT NOT NULL AUTO_INCREMENT,
              endereco_hash VARCHAR(64) NOT NULL,
              query VARCHAR(255) NULL,
              status VARCHAR(40) NULL,
              provider VARCHAR(40) NULL,
              lat DECIMAL(10,7) NULL,
              lng DECIMAL(10,7) NULL,
              formatted_address VARCHAR(255) NULL,
              updated_at DATETIME NULL,
              PRIMARY KEY (id),
              UNIQUE KEY uq_geocoding_cache_hash (endereco_hash),
              INDEX idx_geocoding_cache_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS encontristas_geo (
              id INT NOT NULL AUTO_INCREMENT,
              encontrista_id INT NOT NULL,
              endereco_original VARCHAR(255) NULL,
              endereco_normalizado VARCHAR(255) NULL,
              endereco_hash VARCHAR(64) NULL,
              formatted_address VARCHAR(255) NULL,
              geo_lat DECIMAL(10,7) NULL,
              geo_lng DECIMAL(10,7) NULL,
              geocode_status VARCHAR(40) NULL,
              geocode_source VARCHAR(40) NULL,
              geocode_updated_at DATETIME NULL,
              PRIMARY KEY (id),
              UNIQUE KEY uq_encontristas_geo_encontrista (encontrista_id),
              INDEX idx_encontristas_geo_hash (endereco_hash),
              CONSTRAINT fk_encontristas_geo_encontrista
                FOREIGN KEY (encontrista_id) REFERENCES encontristas(id)
                ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        encontristas_missing = {
            "apelidos": "ALTER TABLE encontristas ADD COLUMN apelidos JSON NULL AFTER nome_usual_ela",
        }
        for col, stmt in encontristas_missing.items():
            if not _column_exists(cur, "encontristas", col):
                cur.execute(stmt)

        encontreiros_missing = {
            "casal_id": "ALTER TABLE encontreiros ADD COLUMN casal_id INT NULL AFTER equipe",
        }
        for col, stmt in encontreiros_missing.items():
            if not _column_exists(cur, "encontreiros", col):
                cur.execute(stmt)

        if not _index_exists(cur, "encontreiros", "idx_encontreiros_casal_id"):
            cur.execute("""
                ALTER TABLE encontreiros
                ADD INDEX idx_encontreiros_casal_id (casal_id)
            """)

        circulos_missing = {
            "coord_orig_ele": "ALTER TABLE circulos ADD COLUMN coord_orig_ele VARCHAR(120) NULL",
            "coord_orig_ela": "ALTER TABLE circulos ADD COLUMN coord_orig_ela VARCHAR(120) NULL",
            "situacao": "ALTER TABLE circulos ADD COLUMN situacao VARCHAR(60) NULL",
            "observacao": "ALTER TABLE circulos ADD COLUMN observacao TEXT NULL",
            "created_at": "ALTER TABLE circulos ADD COLUMN created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
        }
        for col, stmt in circulos_missing.items():
            if not _column_exists(cur, "circulos", col):
                cur.execute(stmt)

        conn.commit()
        return {"ok": True, "msg": "Schema verificado/atualizado com sucesso."}

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass