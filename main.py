import requests
import json
import urllib
import psycopg2
import time


token = 'MGRiYTgwOWUtZDQwMy00ODE0LTllMDQtYTAxMmI4ODc0Njc4'

DB_NAME = 'cxwzeeig'
DB_USER = 'cxwzeeig'
DB_PASS = 'KSuRSCVf0zrHvvnEAyd-TN8clg_ihoaR'
DB_HOST = 'hattie.db.elephantsql.com'
DB_PORT = '5432'


conn = psycopg2.connect(
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT
)

print("Подключен к БД!")

cur = conn.cursor()

# cur.execute("""
#
# CREATE TABLE Orders
# (
# NUMBER TEXT NOT NULL,
# DATE TEXT NOT NULL,
# LASTCHANGEDATE TEXT NOT NULL,
# INCOMEID TEXT NOT NULL,
# ODID TEXT NOT NULL,
# NMID TEXT NOT NULL,
# QUANTITY INT NOT NULL,
# TOTALPRICE FLOAT NOT NULL,
# DISCOUNTPERCENT FLOAT NOT NULL,
# SUBJECT TEXT NOT NULL,
# CATEGORY TEXT NOT NULL,
# BRAND TEXT NOT NULL,
# SUPPLIERARTICLE TEXT NOT NULL,
# TECHSIZE TEXT NOT NULL,
# WAREHOUSENAME TEXT NOT NULL,
# REGION TEXT NOT NULL,
# BARCODE TEXT NOT NULL,
# ISCANCEL TEXT NOT NULL,
# CANCELDT TEXT NOT NULL,
# GNUMBER TEXT NOT NULL
# )
#
# """)
#
# cur.execute("""
#
# CREATE TABLE Sales
# (
# NUMBER TEXT NOT NULL,
# DATE TEXT NOT NULL,
# LASTCHANGEDATE TEXT NOT NULL,
# INCOMEID TEXT NOT NULL,
# ODID TEXT NOT NULL,
# NMID TEXT NOT NULL,
# QUANTITY INT NOT NULL,
# TOTALPRICE FLOAT NOT NULL,
# DISCOUNTPERCENT FLOAT NOT NULL,
# FORPAY FLOAT NOT NULL,
# PROMODISCOUNT FLOAT NOT NULL,
# DISCPRICE FLOAT NOT NULL,
# FINPRICE FLOAT NOT NULL,
# SUBJECT TEXT NOT NULL,
# CATEGORY TEXT NOT NULL,
# BRAND TEXT NOT NULL,
# SUPPLIERARTICLE TEXT NOT NULL,
# TECHSIZE TEXT NOT NULL,
# WAREHOUSENAME TEXT NOT NULL,
# COUNTRY TEXT NOT NULL,
# REGION TEXT NOT NULL,
# OBLASTOKRUGNAME TEXT NOT NULL,
# BARCODE TEXT NOT NULL,
# GNUMBER TEXT NOT NULL,
# ISSUPPLY TEXT NOT NULL,
# ISREALIZATION TEXT NOT NULL,
# ORDERID TEXT NOT NULL,
# SALEID TEXT NOT NULL,
# SPP TEXT NOT NULL,
# ISSTORNO TEXT NOT NULL
# )
#
# """)
#
# conn.commit()
# cur.close()


def get_supplier_info():

    """
    Сгенерировать данные поставщиков
    :json_response_orders: Заказы
    :json_response_sales: Продажи
    """

    response_orders = urllib.request.urlopen('https://suppliers-stats.wildberries.ru/api/v1/supplier/orders?dateFrom='
                                      '2017-03-25T21:00:00.000Z&flag=0&key={}'.format(token)).read()

    json_response_orders = json.loads(response_orders.decode('utf-8'))

    time.sleep(4)

    for elem in json_response_orders:
        if int(elem['date'][8:10]) <= 6:
            try:
                cur.execute(
                    'INSERT INTO Orders (NUMBER, DATE, LASTCHANGEDATE, SUPPLIERARTICLE, TECHSIZE, BARCODE, QUANTITY, TOTALPRICE,'
                    'DISCOUNTPERCENT, WAREHOUSENAME, REGION, INCOMEID, ODID, NMID, SUBJECT, CATEGORY, BRAND, ISCANCEL, CANCELDT,'
                    'GNUMBER) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        [elem['number'], str(elem['date']), elem['lastChangeDate'], elem['supplierArticle'], elem['techSize'],
                                elem['barcode'], elem['quantity'], elem['totalPrice'], elem['discountPercent'],
                                elem['warehouseName'], elem['oblast'], elem['odid'], elem['nmId'],
                                elem['subject'], elem['category'], str(elem['brand']).replace(" ", "_"), elem['isCancel'],
                                elem['cancel_dt'], elem['gNumber']])  # elem['incomeID'],
                conn.commit()
                print(elem['date'], ', ', 'Данные заказов успешно выгружены!')
            except:
                print("Ошибка - возможен дубликат данных!")

    response_sales = urllib.request.urlopen('https://suppliers-stats.wildberries.ru/api/v1/supplier/orders?dateFrom='
                                            '2017-03-25T21:00:00.000Z&flag=0&key={}'.format(token)).read()
    json_response_sales = json.loads(response_sales.decode('utf-8'))
    time.sleep(4)

    for elem in json_response_sales:
        if int(elem['date'][8:10]) <= 6:
            try:
                cur.execute(
                    'INSERT INTO Orders (NUMBER, DATE, LASTCHANGEDATE, SUPPLIERARTICLE, TECHSIZE, BARCODE, QUANTITY, TOTALPRICE,'
                    'DISCOUNTPERCENT, ISSUPPLY, ISREALIZATION, ORDERID, PROMODISCOUNT, WAREHOUSENAME, COUNTRY, OBLASTOKRUGNAME,'
                    'REGION, INCOMEID, SALEID, ODID, SPP, FORPAY, FINPRICE, DISCPRICE, NMID, SUBJECT, CATEGORY, BRAND, ISSTORNO,'
                    'GNUMBER) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    [elem['number'], str(elem['date']), elem['lastChangeDate'], elem['supplierArticle'], elem['techSize'],
                                elem['barcode'], elem['quantity'], elem['totalPrice'], elem['discountPercent'],
                                elem['isSupply'], elem['isRealization'], elem['orderId'], elem['promocodeDiscount'],
                                elem['warehouseName'], elem['countryName'], elem['oblastOkrugName'], elem['regionName'],
                                elem['saleID'], elem['odid'], elem['spp'], elem['forPay'],
                                elem['finishedPrice'], elem['priceWithDisc'], elem['nmId'], elem['subject'], elem['category'],
                                str(elem['brand']).replace(" ", "_"), elem['IsStorno'], elem['gNumber']])  # elem['incomeID'],
                conn.commit()
                print(elem['date'], ', ', 'Данные заказов успешно выгружены!')
            except:
                print("Ошибка - возможен дубликат данных!")


if __name__ == '__main__':
    get_supplier_info()
    conn.close()
    print('Закончено')
