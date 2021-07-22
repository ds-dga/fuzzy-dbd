from db import Database

BASE_DIR = '.'
db = Database()


def find_similarity(name, province, zipcode):
    cur = db.get_cursor()
    q = f'''SELECT
        id,
        name,
        LEVENSHTEIN(name, '{name}') as diff,
        SIMILARITY(name, '{name}') as similarity
    FROM company
    WHERE
        dbd_id = ''
        AND (
            province='{province}'
            OR zipcode='{zipcode}'
        )
        AND 0.55 < SIMILARITY(name, '{name}')
        AND LEVENSHTEIN(name, '{name}') < 12
    ORDER BY similarity DESC, diff ASC;
    '''
    cur.execute(q)
    records = cur.fetchall()
    return records


def main():
    q = '''SELECT id, name, province, zipcode
    FROM company
    WHERE dbd_id != ''
    '''
    cur = db.get_cursor()
    cur.execute(q)
    records = cur.fetchall()
    for pid, name, province, zipcode in records:
        sim_items = find_similarity(name, province, zipcode)
        if len(sim_items) > 0:
            print(f'\r{name:40s} ================================= ', flush=True)
            for rec in sim_items:
                print(rec)
            dups = [
                (pid, sid, _diff, _similarity)
                for sid, _name, _diff, _similarity in sim_items
            ]
            db.insert_bulk_dup(dups)
        else:
            print(f'{name:40s} ========== NOT FOUND', end='\r', flush=True)

if __name__ == '__main__':
    main()

