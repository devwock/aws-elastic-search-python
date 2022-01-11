import time

import settings
from es.es_product import EsProduct
from model.product import Product


class Main:

    def run(self):
        print('===== START ES PRODUCT =====')
        es_product = self.init_es_product()
        product = self.create_product()
        self.delete_es_product(es_product, product)
        self.create_es_product(es_product, product)
        self.query_es_product(es_product, product)
        product = self.update_product(product)
        self.update_es_product(es_product, product)
        self.delete_es_product(es_product, product)
        print('===== END ES PRODUCT =====')

    def init_es_product(self):
        return EsProduct()

    def create_product(self):
        print('----- START CREATE PRODUCT -----')
        product = Product(
            id=2,
            name='devwock',
            categories=['껌'],
            pet_type=Product.PET_TYPE_DOG
        )
        print(
            f'PRODUCT: ID: {product.id}, NAME: {product.name}, CATEGORIES: {product.categories}, '
            f'PET_TYPE: {product.pet_type}, CREATED_AT: {product.created_at}'
        )
        print('----- END CREATE PRODUCT -----')
        return product

    def update_product(self, product):
        print('----- START UPDATE PRODUCT -----')
        product.name = 'devwock2'
        product.pet_type = Product.PET_TYPE_CAT
        print(
            f'PRODUCT: ID: {product.id}, NAME: {product.name}, CATEGORIES: {product.categories}, '
            f'PET_TYPE: {product.pet_type}, CREATED_AT: {product.created_at}'
        )
        print('----- END UPDATE PRODUCT -----')
        return product

    def create_es_product(self, es_product, product):
        print('----- START CREATE ES PRODUCT -----')
        es_product.create_product(product)
        time.sleep(settings.SLEEP_TIME)
        print('----- END CREATE ES PRODUCT -----')

    def update_es_product(self, es_product, product):
        print('----- START UPDATE ES PRODUCT -----')
        es_product.update_product(product)
        time.sleep(settings.SLEEP_TIME)
        print('----- END UPDATE ES PRODUCT -----')

    def delete_es_product(self, es_product, product):
        print('----- START DELETE ES PRODUCT -----')
        es_product.delete_product(product)
        time.sleep(settings.SLEEP_TIME)
        print('----- END DELETE ES PRODUCT -----')

    def query_es_product(self, es_product, product):
        print('----- START QUERY ES PRODUCT -----')
        print('find_by_product_name:', es_product.find_by_product_name(product.name))
        print('count_by_product_name:', es_product.count_by_product_name(product.name))
        print('find_by_keyword:', es_product.find_by_keyword(pet_type=Product.PET_TYPE_DOG, keyword=product.name))
        print('find_by_product:', es_product.find_by_product(product))
        print('----- END QUERY ES PRODUCT -----')


if __name__ == '__main__':
    main = Main()
    main.run()
