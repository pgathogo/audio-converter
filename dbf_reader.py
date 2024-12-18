import sys
import json

def get_raw_dbf_data(dbf: str) -> list:
    HEADER = 1379  #bytes
    data = []
    with open(dbf, "rb") as dbf:
        header_data = dbf.read(HEADER).hex()
        CODE = 4 
        CODE_TITLE_DIST = 52
        TITLE = 64
        ARTIST = 64 
        NEXT_RECORD = 262
        rec_count = 0
        while True:
            record = {}
            record['code'] = dbf.read(CODE).hex()
            dbf.read(CODE_TITLE_DIST).hex()
            record['title'] = dbf.read(TITLE).hex()
            record['artist'] = dbf.read(ARTIST).hex()
            data.append(record)

            nr = dbf.read(NEXT_RECORD)

            if not nr:
                break

            rec_count += 1

    return data

def format_raw_data(hex_data: list, dbf_name: str) -> list:
    # Remove "DBF/" and .DBF from dbf name
    dbf_name = dbf_name[8:-4]
    category = dbf_name

    ascii_data = []
    for i, data in enumerate(hex_data):
        record = {}
        try:
            code_byte_string = bytes.fromhex(data['code'])
            record['code'] = code_byte_string.decode('ASCII')
        except:
            record['code'] = ''

        try:
            title_byte_string = bytes.fromhex(data['title'])
            record['title'] = title_byte_string.decode('ASCII').rstrip()
        except:
            record['title'] = ''

        try:
            art_byte_string = bytes.fromhex(data['artist'])
            record['artist'] = art_byte_string.decode('ASCII').rstrip()
        except:
            record['artist'] = ''

        record['category'] = category

        if record['code'] == '':
            continue

        record['audio_file'] = f"{dbf_name}{record['code']}.MTS"


        ascii_data.append(record)

    return ascii_data

def get_data(dbf: str)-> list:
    raw_data = get_raw_dbf_data(dbf)
    data = format_raw_data(raw_data, dbf)
    return data
