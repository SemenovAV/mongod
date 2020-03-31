import csv
import re
from datetime import datetime
from pprint import pprint

from pymongo import MongoClient, ASCENDING, DESCENDING

client = MongoClient()
database = client['netology']


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as f:
        reader = csv.DictReader(f)
        collection = db['Tickets']
        result = []
        for r in reader:
            r['Дата'] = datetime.strptime(f'{r["Дата"]}.{2020}', "%d.%m.%Y")
            r['Цена'] = int(r['Цена'])
            result.append(r)
        return collection.insert_many(result).inserted_ids


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    return list(db['Tickets'].find().sort('Цена', ASCENDING))


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """

    regex = re.compile(re.escape(name), re.IGNORECASE)
    return list(db['Tickets'].find({'Исполнитель': regex}).sort('Цена', ASCENDING))


def find_by_date(db, sort='asc', **kwargs, ):
    """
    Найти билеты в диапозоне дат с сортировкой по возрастанию или убыванию.
    :param db: база
    :param sort: str -сортировка,
     возможные значения: 'asc' - по возрастанию(по умолчанию), 'desc' - по убыванию

    :param kwargs:
           date_from: Дата,начинаю с которой (включительно) будет произведена выборка с базы.Строка формата - дд.мм.гггг.
           date_to: Дата по которую (включительно) будет произведена выборка с базы.Строка формата - дд.мм.гггг.

    :return: Список словарей.
    """
    query = {}
    sorting_params = {
        'asc': ASCENDING,
        'desc': DESCENDING
    }[sort]

    if kwargs.get('date_from'):
        query['$gte'] = datetime.strptime(kwargs['date_from'], '%d.%m.%Y')
    if kwargs.get('date_to'):
        query['$lte'] = datetime.strptime(kwargs['date_to'], '%d.%m.%Y')

    return list(db['Tickets'].find({'Дата': query}).sort('Дата', sorting_params))


if __name__ == '__main__':
    read_data('artists.csv', database)
    pprint(find_by_date(database, sort='asc', date_from='1.07.2020', date_to='30.07.2020'))
