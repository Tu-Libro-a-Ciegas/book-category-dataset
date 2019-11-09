import argparse
import os
import json
import requests
import pandas as pd
from google.cloud import bigquery

parser = argparse.ArgumentParser(description='Uploads book dataset to BQ')

parser.add_argument(
    '--lang',
    dest='lang',
    type=str,
    default='es',
    help='provide a language code for the description of the books')

args = parser.parse_args()
lang = args.lang

tlac_categories = ['business', 'philosophy', 'science',
                   'poetry', 'anthropology', 'art',
                   'sociology', 'education', 'history',
                   'literature', 'psychology', 'theater']


class book_category_iterator():
    def __init__(self, category):
        self.category = category
        self.start_index = 0
        self.done = False
        self.book_list = []

    def search_books_by_category(self):
        response = requests.get('https://www.googleapis.com/books/v1/volumes?q=subject:' +
                                self.category + '&langRestrict=' + lang + '&startIndex=' + str(self.start_index) + '&maxResults=40')
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


def book_list_to_df(book_list, cat):
    titles = []
    descs = []
    cats = [cat for num in range(len(book_list))]

    for book in book_list:
        titles.append(book.get('title'))
        descs.append(book.get('description'))

    book_dict = {'category': cats,
                 'title': titles,
                 'description': descs}
    return pd.DataFrame.from_dict(book_dict)


to_upload_df = pd.DataFrame(columns=['category', 'title', 'description'])

for cate in tlac_categories:
    bki = book_category_iterator(cate)
    bki.main_loop()
    bl = bki.book_list
    to_upload_df = to_upload_df.append(book_list_to_df(bl, cate))

to_upload_df.reset_index(drop=True, inplace=True)


def append_table(table_name, df):
    client = bigquery.Client()
    dataset_ref = client.dataset('book_backend')
    table_ref = dataset_ref.table(table_name)
    client.load_table_from_dataframe(df, table_ref).result()


append_table('train_categories_' + lang, to_upload_df)
