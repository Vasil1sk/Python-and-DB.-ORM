import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
from models import create_tables, Stock, Book, Publisher, Shop, Sale
import json

def main():
    with open("DB_data.json", "r") as f:
        db_data = json.load(f)
        driver = db_data["driver"]
        login = db_data["login"]
        password = db_data["password"]
        server_name = db_data["server_name"]
        server_port = db_data["server_port"]
        db_name = db_data["db_name"]

    DSN = f"{driver}://{login}:{password}@{server_name}:{server_port}/{db_name}"
    engine = sq.create_engine(DSN)

    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    with open('fixtures/tests_data.json', 'r') as fd:
        data = json.load(fd)

    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
        session.flush()

    session.commit()
    session.close()
def search_publishers_books(name):
    with open("DB_data.json", "r") as f:
        db_data = json.load(f)
        driver = db_data["driver"]
        login = db_data["login"]
        password = db_data["password"]
        server_name = db_data["server_name"]
        server_port = db_data["server_port"]
        db_name = db_data["db_name"]

    DSN = f"{driver}://{login}:{password}@{server_name}:{server_port}/{db_name}"
    engine = sq.create_engine(DSN)

    Session = sessionmaker(bind=engine)
    session = Session()

    publisher_name = name

    publisher = session.query(Publisher).filter_by(name=publisher_name).first()

    if not publisher:
        print(f"Издатель '{publisher_name}' не найден.")
    else:
        sales = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).\
            join(Stock, Book.id == Stock.id_book).\
            join(Sale, Stock.id == Sale.id_stock).\
            join(Shop, Shop.id == Stock.id_shop).\
            filter(Book.id_publisher == publisher.id).all()

        if not sales:
            print(f"Издатель '{publisher_name}' не имеет продаж.")
        else:
            print("Название книги | Название магазина | Стоимость покупки | Дата покупки")
            for sale in sales:
                print(f"{sale.title} | {sale.name} | {sale.price} | {sale.date_sale}")

    session.commit()
    session.close()

if __name__ == "__main__":
    main()
    search_publishers_books("O’Reilly")