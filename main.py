import csv

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, CollectionInvalid


def get_db_handle(db_name, host, port, password, username):
    client = MongoClient(host=host,
                         port=int(port),
                         username=username,
                         password=password,
                         )
    db_handle = client[db_name]
    return db_handle, client


def get_collection_handle(db_handle, collection_name):
    return db_handle[collection_name]


def updater(medium_q_link, snapshot, collection_handler):
    """
    update record with given key , add given snapshot
    :param medium_q_link:
    :param snapshot:
    :param collection_handler:
    :return:
    """
    collection_handler.update_one({"medium_q_link": medium_q_link}, {"$set": {"snapshot": snapshot}})
    raise ValueError(f"duplicate key found : {medium_q_link}")


def excel_reader(file_name, collection_handler):
    """
    read data in give csv file and pass data to updater
    :param file_name:
    :param path_to_save:
    :return:
    """
    try:
        with open(file_name) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader)
            try:
                for row in csv_reader:
                    updater(row[8], row[10], collection_handler)
                print("records updated successfully")
            except Exception as e:
                print(str(e))
    except Exception as err:
        print(err)


if __name__ == "__main__":
    print("Enter hots port username password and csv file_name : ")
    host = input()
    port = input()
    username = input()
    password = input()
    file_name = input()
    try:
        db_handler, mongo_client = get_db_handle('EducationStore', host, port, username, password)
        collection_handler = get_collection_handle(db_handler, 'main_files')
        excel_reader(file_name, collection_handler)
    except ValueError as e:
        print(e)
    except ConnectionFailure:
        print("Could not connect to MongoDB")
    except CollectionInvalid:
        print("Could not find the Collection")
