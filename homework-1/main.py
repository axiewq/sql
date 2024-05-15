"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
with open('/Users/max/PycharmProjects/postgres-homeworks/homework-1/north_data/customers_data.csv', 'r', encoding='utf-8') as file:
    customers_data = [row for row in csv.reader(file) if 'customer_id' not in row]

with open('/Users/max/PycharmProjects/postgres-homeworks/homework-1/north_data/employees_data.csv', 'r', encoding='utf-8') as file:
    employees_data = [row for row in csv.reader(file) if 'employee_id' not in row]

with open('/Users/max/PycharmProjects/postgres-homeworks/homework-1/north_data/orders_data.csv', 'r', encoding='utf-8') as file:
    orders_data = [row for row in csv.reader(file) if 'order_id' not in row]

connect = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='230419',
    port='5432'
)

cur = connect.cursor()

for customer in customers_data:
    cur.execute('''INSERT INTO customers (customer_id, company_name, contact_name)
    VALUES(%s, %s, %s)
    ''', customer)
connect.commit()

for employee in employees_data:
    cur.execute('''INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes)
    VALUES(%s, %s, %s, %s, %s, %s)
    ''', employee)
connect.commit()

for order in orders_data:
    cur.execute('''INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city)
    VALUES(%s, %s, %s, %s, %s)
    ''', order)
connect.commit()
connect.close()



