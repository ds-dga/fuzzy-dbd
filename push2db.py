import csv
import os
from datetime import date
from db import Database

BASE_DIR = '.'
db = Database()

def txt_cleanup(txt):
    return ' '.join(txt.split())

def t0_factory():
    result = []
    with open('./fti_public_t0_factory_master-roots180664.csv', 'rt') as f:
        cf = csv.DictReader(f)
        for row in cf:
            # print(row)
            if not row['fname']: continue
            name = row['fname'] if len(row['fname']) > 3 else row['oname']
            item = {
                'name': txt_cleanup(name),
                'district': txt_cleanup(row['district']),
                'province': txt_cleanup(row['province']),
                'zipcode': txt_cleanup(row['postcode']),
                'c_code': txt_cleanup(row['ccode']),
            }
            result.append(item)
    return result

def sixty_file(fp):
    result = []
    with open(fp, 'rt', encoding='cp874') as f:
        cf = csv.DictReader(f)
        for row in cf:
            # print(row)
            item = {
                'dbd_id': row['เลขทะเบียน'].replace("'", ''),
                'name': txt_cleanup(row['ชื่อนิติบุคคล']),
                'found_on': txt_cleanup(row['วันที่จดทะเบียน']),
                'district': txt_cleanup(row['อำเภอ']),
                'province': txt_cleanup(row['จังหวัด']),
                'zipcode': txt_cleanup(row['รหัสไปรษณีย์']),
            }
            # print(item)
            result.append(item)
    return result


def sixty_proc():
    items = []
    for b, _, fs in os.walk(os.path.join(BASE_DIR, '60')):
        print(fs)
        for ff in fs:
            if ff.find('DV_') != 0: continue
            if ff.find('_1.csv') > 0: continue
            print(ff)
            fp = os.path.join(b, ff)
            try:
                items += sixty_file(fp)
            except UnicodeDecodeError as e:
                print(fp, e)

    return items

def push2db(items):
    # the only different is to make it list of list instead of list of dict
    # name,district,province,zipcode,dbd_id,c_code,found_on
    count = 0
    one_set = []
    for item in items:
        found_on = None
        if 'found_on' in item:
            dd, mm, yyyy = item['found_on'].split('/')
            try:
                date(int(yyyy)-543, int(mm), int(dd))
                found_on = f'{int(yyyy)-543}-{mm}-{dd}'
            except:
                pass
        one_set.append([
            item['name'],
            item['district'],
            item['province'],
            item['zipcode'],
            item['dbd_id'] if 'dbd_id' in item else '',
            item['c_code'] if 'c_code' in item else '',
            found_on,
        ])
        count += 1

        if len(one_set) > 500:
            print(f'inserting ... {count:10d}', end='\r', flush=True)
            db.insert_bulk_records(one_set)
            one_set = []

    print(f'inserting ... {len(one_set)}', end='\r', flush=True)
    db.insert_bulk_records(one_set)

def main():
    print('=============== T0 ===============')
    items = t0_factory()
    print(f'Total: {len(items):5d}')
    push2db(items)
    print('=============== 60 ===============')
    items = sixty_proc()
    print(f'Total: {len(items):5d}')
    # sixty_file('./60/DV_10-64-117150_004.csv')
    push2db(items)


if __name__ == '__main__':
    main()

