from db import db_conn


def _column_exists(cur, table_name, column_name):
    cur.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (column_name,))
    return cur.fetchone() is not None


def _index_exists(cur, table_name, index_name):
    cur.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name = %s", (index_name,))
    return cur.fetchone() is not None


def _foreign_key_exists(cur, table_name, constraint_name):
    cur.execute("""
        SELECT CONSTRAINT_NAME
        FROM information_schema.TABLE_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND CONSTRAINT_NAME = %s
          AND CONSTRAINT_TYPE = 'FOREIGN KEY'
        LIMIT 1
    """, (table_name, constraint_name))
    return cur.fetchone() is not None


def _ensure_paroquia_id(cur, table_name, after_column="id"):
    if not _column_exists(cur, table_name, "paroquia_id"):
        cur.execute(f"""
            ALTER TABLE {table_name}
            ADD COLUMN paroquia_id INT NULL AFTER {after_column}
        """)

    cur.execute(f"""
        UPDATE {table_name}
        SET paroquia_id = 1
        WHERE paroquia_id IS NULL
    """)

    cur.execute(f"""
        ALTER TABLE {table_name}
        MODIFY paroquia_id INT NOT NULL
    """)

    idx_name = f"idx_{table_name}_paroquia"
    if not _index_exists(cur, table_name, idx_name):
        cur.execute(f"""
            ALTER TABLE {table_name}
            ADD INDEX {idx_name} (paroquia_id)
        """)


def _ensure_paroquia_fk(cur, table_name, constraint_name):
    if not _foreign_key_exists(cur, table_name, constraint_name):
        cur.execute(f"""
            ALTER TABLE {table_name}
            ADD CONSTRAINT {constraint_name}
            FOREIGN KEY (paroquia_id) REFERENCES paroquias(id)
        """)


def ensure_database_schema():
    conn = db_conn()
    cur = conn.cursor()

    try:
        # =========================
        # PARÓQUIAS
        # =========================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS paroquias (
              id INT NOT NULL AUTO_INCREMENT,
              nome VARCHAR(150) NOT NULL,
              cidade VARCHAR(100) NULL,
              estado VARCHAR(2) NULL,
              diocese VARCHAR(150) NULL,
              ativa TINYINT(1) NOT NULL DEFAULT 1,
              created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              INDEX idx_paroquias_ativa (ativa),
              INDEX idx_paroquias_nome (nome)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        cur.execute("""
            INSERT IGNORE INTO paroquias
                (id, nome, cidade, estado, diocese, ativa)
            VALUES
                (1, 'Paróquia Divino Espírito Santo', 'Maceió', 'AL', 'Arquidiocese de Maceió', 1)
        """)

        # =========================
        # ENCONTRISTAS
        # =========================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS encontristas (
              id INT NOT NULL AUTO_INCREMENT,
              paroquia_id INT NOT NULL DEFAULT 1,
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
              INDEX idx_encontristas_paroquia (paroquia_id),
              INDEX idx_encontristas_ano (ano),
              INDEX idx_encontristas_nome (nome_usual_ele, nome_usual_ela),
              CONSTRAINT fk_encontristas_paroquia
                FOREIGN KEY (paroquia_id) REFERENCES paroquias(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # =========================
        # ENCONTREIROS
        # =========================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS encontreiros (
              id INT NOT NULL AUTO_INCREMENT,
              paroquia_id INT NOT NULL DEFAULT 1,
              ano INT NOT NULL,
              equipe VARCHAR(120) NOT NULL,
              casal_id INT NULL,
              coordenador VARCHAR(10) NOT NULL DEFAULT 'Não',
              observacao TEXT NULL,
              status VARCHAR(40) NULL,
              PRIMARY KEY (id),
              INDEX idx_encontreiros_paroquia (paroquia_id),
              INDEX idx_encontreiros_ano (ano),
              INDEX idx_encontreiros_equipe (equipe),
              INDEX idx_encontreiros_casal_id (casal_id),
              CONSTRAINT fk_encontreiros_paroquia
                FOREIGN KEY (paroquia_id) REFERENCES paroquias(id),
              CONSTRAINT fk_encontreiros_encontrista
                FOREIGN KEY (casal_id) REFERENCES encontristas(id)
                ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # =========================
        # PALESTRAS
        # =========================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS palestras (
              id INT NOT NULL AUTO_INCREMENT,
              paroquia_id INT NOT NULL DEFAULT 1,
              ano INT NOT NULL,
              palestra VARCHAR(255) NOT NULL,
              casal_id INT NULL,
              palestrante VARCHAR(255) NULL,
              observacao TEXT NULL,
              status VARCHAR(40) NULL,
              PRIMARY KEY (id),
              INDEX idx_palestras_paroquia (paroquia_id),
              INDEX idx_palestras_ano (ano),
              INDEX idx_palestras_palestra (palestra),
              INDEX idx_palestras_casal_id (casal_id),
              CONSTRAINT fk_palestras_paroquia
                FOREIGN KEY (paroquia_id) REFERENCES paroquias(id),
              CONSTRAINT fk_palestras_encontrista
                FOREIGN KEY (casal_id) REFERENCES encontristas(id)
                ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # =========================
        # CÍRCULOS
        # =========================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS circulos (
              id INT NOT NULL AUTO_INCREMENT,
              paroquia_id INT NOT NULL DEFAULT 1,
              ano INT NOT NULL,
              nome_circulo VARCHAR(120) NULL,
              cor_circulo VARCHAR(60) NULL,
              integrantes_original TEXT NULL,
              integrantes_atual TEXT NULL,
              coord_orig_casal_id INT NULL,
              coord_atual_casal_id INT NULL,
              situacao VARCHAR(60) NULL,
              observacao TEXT NULL,
              created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              INDEX idx_circulos_paroquia (paroquia_id),
              INDEX idx_circulos_ano (ano),
              INDEX idx_circulos_coord_orig_casal_id (coord_orig_casal_id),
              INDEX idx_circulos_coord_atual_casal_id (coord_atual_casal_id),
              CONSTRAINT fk_circulos_paroquia
                FOREIGN KEY (paroquia_id) REFERENCES paroquias(id),
              CONSTRAINT fk_circulos_coord_orig_casal
                FOREIGN KEY (coord_orig_casal_id) REFERENCES encontristas(id)
                ON DELETE SET NULL,
              CONSTRAINT fk_circulos_coord_atual_casal
                FOREIGN KEY (coord_atual_casal_id) REFERENCES encontristas(id)
                ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # =========================
        # CACHE GEOCODING
        # =========================
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

        # =========================
        # ENCONTRISTAS GEO
        # =========================
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

        # =========================
        # COLUNAS FALTANTES - ENCONTRISTAS
        # =========================
        encontristas_missing = {
            "paroquia_id": "ALTER TABLE encontristas ADD COLUMN paroquia_id INT NULL AFTER id",
            "apelidos": "ALTER TABLE encontristas ADD COLUMN apelidos JSON NULL AFTER nome_usual_ela",
        }

        for col, stmt in encontristas_missing.items():
            if not _column_exists(cur, "encontristas", col):
                cur.execute(stmt)

        # =========================
        # COLUNAS FALTANTES - ENCONTREIROS
        # =========================
        encontreiros_missing = {
            "paroquia_id": "ALTER TABLE encontreiros ADD COLUMN paroquia_id INT NULL AFTER id",
            "casal_id": "ALTER TABLE encontreiros ADD COLUMN casal_id INT NULL AFTER equipe",
        }

        for col, stmt in encontreiros_missing.items():
            if not _column_exists(cur, "encontreiros", col):
                cur.execute(stmt)

        # =========================
        # COLUNAS FALTANTES - PALESTRAS
        # =========================
        palestras_missing = {
            "paroquia_id": "ALTER TABLE palestras ADD COLUMN paroquia_id INT NULL AFTER id",
            "casal_id": "ALTER TABLE palestras ADD COLUMN casal_id INT NULL AFTER palestra",
            "palestrante": "ALTER TABLE palestras ADD COLUMN palestrante VARCHAR(255) NULL AFTER casal_id",
        }

        for col, stmt in palestras_missing.items():
            if not _column_exists(cur, "palestras", col):
                cur.execute(stmt)

        # =========================
        # COLUNAS FALTANTES - CÍRCULOS
        # =========================
        circulos_missing = {
            "paroquia_id": "ALTER TABLE circulos ADD COLUMN paroquia_id INT NULL AFTER id",
            "coord_orig_casal_id": "ALTER TABLE circulos ADD COLUMN coord_orig_casal_id INT NULL AFTER cor_circulo",
            "coord_atual_casal_id": "ALTER TABLE circulos ADD COLUMN coord_atual_casal_id INT NULL AFTER coord_orig_casal_id",
            "situacao": "ALTER TABLE circulos ADD COLUMN situacao VARCHAR(60) NULL",
            "observacao": "ALTER TABLE circulos ADD COLUMN observacao TEXT NULL",
            "created_at": "ALTER TABLE circulos ADD COLUMN created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
        }

        for col, stmt in circulos_missing.items():
            if not _column_exists(cur, "circulos", col):
                cur.execute(stmt)

        # =========================
        # GARANTIR paroquia_id
        # =========================
        _ensure_paroquia_id(cur, "encontristas")
        _ensure_paroquia_id(cur, "encontreiros")
        _ensure_paroquia_id(cur, "palestras")
        _ensure_paroquia_id(cur, "circulos")

        # =========================
        # ÍNDICES
        # =========================
        if not _index_exists(cur, "encontreiros", "idx_encontreiros_casal_id"):
            cur.execute("""
                ALTER TABLE encontreiros
                ADD INDEX idx_encontreiros_casal_id (casal_id)
            """)

        if not _index_exists(cur, "palestras", "idx_palestras_casal_id"):
            cur.execute("""
                ALTER TABLE palestras
                ADD INDEX idx_palestras_casal_id (casal_id)
            """)

        if not _index_exists(cur, "circulos", "idx_circulos_coord_orig_casal_id"):
            cur.execute("""
                ALTER TABLE circulos
                ADD INDEX idx_circulos_coord_orig_casal_id (coord_orig_casal_id)
            """)

        if not _index_exists(cur, "circulos", "idx_circulos_coord_atual_casal_id"):
            cur.execute("""
                ALTER TABLE circulos
                ADD INDEX idx_circulos_coord_atual_casal_id (coord_atual_casal_id)
            """)

        if not _index_exists(cur, "encontristas", "idx_encontristas_paroquia"):
            cur.execute("""
                ALTER TABLE encontristas
                ADD INDEX idx_encontristas_paroquia (paroquia_id)
            """)

        if not _index_exists(cur, "encontreiros", "idx_encontreiros_paroquia"):
            cur.execute("""
                ALTER TABLE encontreiros
                ADD INDEX idx_encontreiros_paroquia (paroquia_id)
            """)

        if not _index_exists(cur, "palestras", "idx_palestras_paroquia"):
            cur.execute("""
                ALTER TABLE palestras
                ADD INDEX idx_palestras_paroquia (paroquia_id)
            """)

        if not _index_exists(cur, "circulos", "idx_circulos_paroquia"):
            cur.execute("""
                ALTER TABLE circulos
                ADD INDEX idx_circulos_paroquia (paroquia_id)
            """)

        # =========================
        # FOREIGN KEYS MULTIPARÓQUIA
        # =========================
        _ensure_paroquia_fk(cur, "encontristas", "fk_encontristas_paroquia")
        _ensure_paroquia_fk(cur, "encontreiros", "fk_encontreiros_paroquia")
        _ensure_paroquia_fk(cur, "palestras", "fk_palestras_paroquia")
        _ensure_paroquia_fk(cur, "circulos", "fk_circulos_paroquia")

        # =========================
        # FOREIGN KEYS ESPECÍFICAS
        # =========================
        if _column_exists(cur, "palestras", "casal_id"):
            if not _foreign_key_exists(cur, "palestras", "fk_palestras_encontrista"):
                cur.execute("""
                    ALTER TABLE palestras
                    ADD CONSTRAINT fk_palestras_encontrista
                    FOREIGN KEY (casal_id) REFERENCES encontristas(id)
                    ON DELETE SET NULL
                """)

        if _column_exists(cur, "encontreiros", "casal_id"):
            if not _foreign_key_exists(cur, "encontreiros", "fk_encontreiros_encontrista"):
                cur.execute("""
                    ALTER TABLE encontreiros
                    ADD CONSTRAINT fk_encontreiros_encontrista
                    FOREIGN KEY (casal_id) REFERENCES encontristas(id)
                    ON DELETE SET NULL
                """)

        if _column_exists(cur, "circulos", "coord_orig_casal_id"):
            if not _foreign_key_exists(cur, "circulos", "fk_circulos_coord_orig_casal"):
                cur.execute("""
                    ALTER TABLE circulos
                    ADD CONSTRAINT fk_circulos_coord_orig_casal
                    FOREIGN KEY (coord_orig_casal_id) REFERENCES encontristas(id)
                    ON DELETE SET NULL
                """)

        if _column_exists(cur, "circulos", "coord_atual_casal_id"):
            if not _foreign_key_exists(cur, "circulos", "fk_circulos_coord_atual_casal"):
                cur.execute("""
                    ALTER TABLE circulos
                    ADD CONSTRAINT fk_circulos_coord_atual_casal
                    FOREIGN KEY (coord_atual_casal_id) REFERENCES encontristas(id)
                    ON DELETE SET NULL
                """)

        conn.commit()
        return {"ok": True, "msg": "Schema verificado/atualizado com sucesso."}

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
