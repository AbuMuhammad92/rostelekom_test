import json
import os
from multiprocessing import Pool, current_process
import psycopg2

# Параметры для подключения к PostgreSQL
DATABASE = "defaultdb"
USER = "defaultuser"
PASSWORD = "defaultpassword"
HOST = "localhost"
PORT = "5432"


# Создание БД
def create_database():
    conn = psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS companies
                     (name TEXT, okved_code TEXT, inn TEXT, kpp TEXT, registration TEXT)''')
    conn.commit()
    conn.close()


# Вставка данных в БД
def insert_into_database(conn, company_info):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO companies (name, okved_code, inn, kpp, registration) VALUES (%s, %s, %s, %s, %s)",
                   (company_info["name"], company_info["okved_code"], company_info["inn"],
                    company_info["kpp"], company_info["registration"]))
    conn.commit()


# Получение города из OGRN
def get_city_from_ogrn(ogrn):
    region_code = ogrn[3:5]
    if region_code == "27":
        return True
    return False


# Обработка одного файла
def process_file(filename):
    print(f"Process {current_process().name} is processing {filename}")

    conn = psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)

    with open(filename, 'r', encoding='utf-8') as file:
        try:
            data = json.load(file)
        except Exception as e:
            pass
        for company in data:
            if "data" in company and "СвОКВЭД" in company["data"] and "СвОКВЭДОсн" in company["data"]["СвОКВЭД"]:
                main_activity = company["data"]["СвОКВЭД"]["СвОКВЭДОсн"]
                if main_activity["КодОКВЭД"] == "62.0":
                    if get_city_from_ogrn(company["ogrn"]):
                        insert_into_database(conn, {
                            "name": company["name"],
                            "okved_code": main_activity["КодОКВЭД"],
                            "inn": company["inn"],
                            "kpp": company["kpp"],
                            "registration": 'Хабаровск'
                        })

    conn.close()


def main():
    data_folder = '/Users/abumuhhamad/PythonProject/test/test/egrul.json'
    create_database()

    all_files = [os.path.join(root, filename) for root, _, files in os.walk(data_folder) for filename in files if
                 filename.endswith('.json')]

    num_processes = 10

    with Pool(num_processes) as pool:
        pool.map(process_file, all_files)


if __name__ == "__main__":
    main()
