from datetime import datetime

from es.base_es import BaseEs
from model.product import Product


class EsProduct(BaseEs):
    _UPDATABLE_LIST = ['product_name', 'categories', 'pet_type']
    _index = 'product_to_search'

    @classmethod
    def _get_query_match_product_id(cls, product_id):
        return cls._get_single_match_query('product_id', product_id)

    @classmethod
    def _get_query_match_product_name(cls, product_name):
        return cls._get_single_match_query('product_name', product_name)

    @classmethod
    def _get_exclude_pet_type_query(cls, pet_type):
        filtered_product_type = None
        filter_query = None
        if pet_type == Product.PET_TYPE_DOG:
            filtered_product_type = Product.PET_TYPE_CAT
        elif pet_type == Product.PET_TYPE_CAT:
            filtered_product_type = Product.PET_TYPE_DOG

        if filtered_product_type:
            filter_query = {
                'bool': {
                    'must_not': [
                        {
                            'term': {
                                'pet_type': filtered_product_type
                            }
                        }
                    ]
                }
            }
        return filter_query

    def find_by_keyword(self, pet_type, keyword: str):
        query = {
            'sort': [
                {'_score': {'order': 'desc'}},
                {'created_at': {'order': 'desc'}}
            ],
            '_source': ['product_id'],
            'query': {
                'bool': {
                    'must': [
                        {'match': {'product_name': {'query': keyword}}},
                        # {'match': {'product_name': {'query': product_name, 'boost': 2}}}
                    ]
                }
            }
        }
        filter_query = EsProduct._get_exclude_pet_type_query(pet_type)
        if filter_query:
            query['query']['bool']['filter'] = [filter_query]
        response = self._find(body=query, size=100)
        return self._parse_product_ids(response)

    def _find_by_product_name(self, product_name):
        return self._find(body=self._get_query_match_product_name(product_name))

    def find_by_product_name(self, product_name):
        response = self._find_by_product_name(product_name)
        return self._parse_find_response(response)

    def _find_by_product(self, product):
        return self._find(body={'query': {'match': {'product_id': product.id}}})

    def find_by_product(self, product):
        response = self._find_by_product(product)
        return self._parse_find_response(response)

    def _count_by_product_name(self, product_name):
        response = self._count(body=self._get_query_match_product_name(product_name))
        return self._parse_count_response(response)

    def count_by_product_name(self, product_name):
        return self._count_by_product_name(product_name)

    def create_product(self, product):
        if self._exists_by_product(product):
            return
        product_dict = self._product_to_dict(product)
        self._es_instance.index(index=self._index, body=product_dict)

    def update_product(self, product):
        if not self._exists_by_product(product):
            return
        query = EsProduct._get_query_match_product_id(product.id)
        script = self._product_to_script(product)
        self._update_by_query(query, script)

    def delete_product(self, product):
        if not self._exists_by_product(product):
            return

        query = EsProduct._get_query_match_product_id(product.id)
        self._delete_by_query(query)

    def _parse_product_ids(self, response):
        hits = self._parse_find_response(response)
        return [hit['product_id'] for hit in hits]

    def _exists_by_product(self, product):
        result = self._find_by_product(product)
        hits = result.get('hits')
        if not hits:
            return False

        inner_hits = hits.get('hits')
        if not inner_hits or len(inner_hits) == 0:
            return False

        return True

    def _product_to_dict(self, product):
        return {
            'product_id': product.id,
            'product_name': product.name,
            'categories': product.categories,
            'pet_type': product.pet_type,
            'created_at': product.created_at
        }

    def _product_to_script(self, product):
        product_dict = self._product_to_dict(product)
        return self._product_dict_to_update_script(product_dict)

    def _product_dict_to_update_script(self, product_dict):
        source_list = []
        for key, value in product_dict.items():
            if key not in self._UPDATABLE_LIST:
                continue

            if type(value) in (int, list, datetime):
                source_list.append(f'ctx._source.{key}={value};')
            else:
                source_list.append(f'ctx._source.{key}="{value}";')

        source = ''.join(source_list)
        return {
            'script': {
                'source': source, 'lang': 'painless'
            }
        }







