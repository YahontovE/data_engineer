import json
import psycopg2


def create_database(database_name, params):
    """Создание базы данных и таблиц для хранения данных."""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE source_data (
                    source_data_id INT GENERATED ALWAYS AS IDENTITY, 
                    data jsonb NOT NULL,
                    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT "source_data_pkey" PRIMARY KEY (source_data_id)
                )
            """)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE hub_post (
                    post_id INT NOT NULL,
                    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT "hub_post_pkey" PRIMARY KEY (post_id)
                )
            """)
    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE hub_user (
                    user_id INT NOT NULL,
                    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT "hub_user_pk" PRIMARY KEY (user_id)
                )
            """)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE link_user_post (
                    id INT GENERATED ALWAYS AS IDENTITY, 
                    post_id INT NOT NULL,
                    user_id INT NOT NULL,
                    CONSTRAINT "link_user_post_pk" PRIMARY KEY (id),
                    CONSTRAINT "link_user_post_post_id_fk" FOREIGN key (post_id) REFERENCES hub_post(post_id),
                    CONSTRAINT "link_user_post_user_id_fk" FOREIGN key (user_id) REFERENCES hub_user(user_id)
                )
            """)
    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE sat_post (
                    post_id INT NOT NULL, 
                    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    title text,
                    body text,
                    source_data_id INT NOT NULL,
                    CONSTRAINT "sat_user_post_pk" PRIMARY KEY (post_id, load_date),
                    CONSTRAINT "sat_user_post_post_id_fk" FOREIGN key (post_id) REFERENCES hub_post(post_id),
                    CONSTRAINT "sat_user_post_source_id_fk" FOREIGN key (source_data_id) REFERENCES source_data(source_data_id)
                )
            """)

    conn.commit()
    conn.close()


def save_in_table(data, database_name, params):
    """Сохранение данных в таблицы"""
    conn = psycopg2.connect(dbname=database_name, **params)
    for item in data:
        with conn.cursor() as cur:
            """Наполнение STG таблицы данными из иточника"""

            cur.execute("INSERT INTO source_data (data) VALUES (%s)", (json.dumps(item),))

    with conn.cursor() as cur:
        """Загрузка данных из STG в HUB"""

        cur.execute("INSERT INTO hub_post (post_id) " +
                    "SELECT DISTINCT (data->>'id')::INT " +
                    "FROM source_data")

    with conn.cursor() as cur:
        """Загрузка данных из STG в HUB"""

        cur.execute("INSERT INTO hub_user (user_id) " +
                    "SELECT DISTINCT (data->>'userId')::INT " +
                    "FROM source_data")

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM source_data")
        temp = cur.fetchall()

    for row in temp:
        rowId = row[0]
        item = row[1]
        with conn.cursor() as cur:
            """Загрузка данных из STG в LINK"""

            cur.execute("INSERT INTO link_user_post (user_id, post_id) " +
                        "VALUES (%s,%s)", (item['userId'], item['id']))

        with conn.cursor() as cur:
            """Загрузка данных из STG в SATELLITE"""

            cur.execute("INSERT INTO sat_post (post_id, title, body, source_data_id) " +
                        "VALUES (%s,%s,%s,%s)", (item['id'], item['title'], item['body'], rowId))

    conn.commit()
    conn.close()
