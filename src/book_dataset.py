import json
import requests


tlac_categories = ['business', 'philosophy', 'science',
                   'poetry', 'anthropology', 'art',
                   'sociology', 'education', 'history',
                   'literature', 'psychology', 'theatre']


class book_category_iterator():
    def __init__(self, category):
        self.category = category
        self.start_index = 0
        self.done = False
        self.book_list = []

    def search_books_by_category(self):
        response = requests.get('https://www.googleapis.com/books/v1/volumes?q=subject:' +
                                self.category + '&langRestrict=es&startIndex=' + str(self.start_index) + '&maxResults=40')
        if response.status_code == 200:
            self.start_index += 40
            return json.loads(response.content.decode('utf-8'))
        else:
            raise Exception(
                f"Response has status code of {response.status_code}")

    def parse_book_info(self, jbook):
        done = False
        books = (book for book in jbook.get('items'))

        while done is False:
            try:
                current_book = next(books)
            except StopIteration:
                done = True

            jbook_dict = {}

            title = current_book.get('volumeInfo').get('title')
            desc = current_book.get('volumeInfo').get('description')
            ctgry = current_book.get('volumeInfo').get('categories')

            if title is None or desc is None or ctgry is None:
                pass
            else:
                jbook_dict.update({
                    'title': title,
                    'description': desc,
                    'categories': ctgry})

                self.book_list.append(jbook_dict)

    def main_loop(self):
        while self.done is False:
            json_books = self.search_books_by_category()
            items = json_books.get('items')
            if items is None:
                self.done = True
                break

            self.parse_book_info(json_books)


'''
fiction = book_category_iterator('fiction')
fiction.main_loop()
'''


class fill_per_category():

    def __init__(self, category):
        self.category = category
        self.info = []

    def run(self):
        process = book_category_iterator(self.category)
        process.main_loop()
        self.info.append(process.book_list)

        return self.info


results = []
for i in tlac_categories:
    rs = fill_per_category(i)
    results.append(rs.run())

# print(results)

# crear pandas dataframe

# import pandas as pd
# from google.cloud import bigquery

# def append_table(table_name, df):
#     client = bigquery.Client()
#     dataset_ref = client.dataset('pqf_lab')
#     table_ref = dataset_ref.table(table_name)
#     client.load_table_from_dataframe(df, table_ref).result()
