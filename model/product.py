from datetime import datetime


class Product:

    id = None
    name = None
    pet_type = None
    categories = None

    PET_TYPE_DOG = 'DOG'
    PET_TYPE_CAT = 'CAT'
    PET_TYPE_ALL = 'ALL'

    def __init__(self, id: int, name: str, pet_type: str, categories: [str]):
        self.id = id
        self.name = name
        self.pet_type = pet_type
        self.categories = categories,
        self.created_at = datetime.now()
        super().__init__()
