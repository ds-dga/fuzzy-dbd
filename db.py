import psycopg2


class Database(object):
    """ """

    def __init__(self, dbconn=None):
        if dbconn is None:
            dbconn = "dbname='dbd' user='sipp11' host='192.168.82.34' port='35432' password='banshee10'"
        self.conn = psycopg2.connect(dbconn)
        self.cursor = self.conn.cursor()

    def create_table_company(self):
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company (
                id serial primary key,
                name text,
                district text,
                province text,
                zipcode text,
                dbd_id text,
                c_code text,
                found_on date NULL,
                UNIQUE (c_code, dbd_id)
            );

            CREATE TABLE IF NOT EXISTS possible_dup (
                id serial primary key,
                parent_id int,
                pair_id int,
                diff int,
                similarity numeric,
                is_matched boolean NULL,
                UNIQUE (parent_id, pair_id)
            );

            """
        )
        self.conn.commit()

    def get_cursor(self):
        return self.cursor

    def close(self):
        self.conn.close()

    def insert_bulk_dup(self, data):
        items = ",".join(
            self.cursor.mogrify(
                "(%s,%s,%s,%s)", x
            ).decode("utf-8")
            for x in data
        )
        q = f"INSERT INTO possible_dup (parent_id,pair_id,diff,similarity) VALUES "
        try:
            self.cursor.execute(f"""{q} {items}
            ON CONFLICT DO NOTHING;
            """)
            self.conn.commit()
        except Exception as e:
            print(e, end='\r')
            self.conn.rollback()

    def insert_dup(
        self,
        parent_id,
        pair_id,
        diff,
        similarity
    ):
        data = [
            (
                parent_id,
                pair_id,
                diff,
                similarity,
            )
        ]
        self.insert_bulk_dup(data)

    def insert_bulk_records(self, data):
        items = ",".join(
            self.cursor.mogrify(
                "(%s,%s,%s,%s,%s,%s,%s)", x
            ).decode("utf-8")
            for x in data
        )
        q = f"INSERT INTO company (name,district,province,zipcode,dbd_id,c_code,found_on) VALUES "
        try:
            self.cursor.execute(f"""{q} {items}
            ON CONFLICT DO NOTHING;
            """)
            self.conn.commit()
        except Exception as e:
            print(e, end='\r')
            self.conn.rollback()


    def insert_record(
        self,
        name,
        district,
        province,
        zipcode,
        dbd_id,
        c_code,
        found_on
    ):
        data = [
            (
                name,
                district,
                province,
                zipcode,
                dbd_id,
                c_code,
                found_on,
            )
        ]
        self.insert_bulk_records(data)
