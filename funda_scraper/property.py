from dataclasses import dataclass, field

@dataclass
class Property():
    url: str = None
    price: str = None
    address = None
    city = None
    description = None
    listed_since=  None
    zip_code = None
    size = None
    year_of_construction = None
    living_area = None
    house_type = None
    building_type = None
    number_of_rooms = None
    number_of_bathrooms = None
    layout = None
    energy_label = None
    insulation = None
    heating = None
    ownership = None
    exteriors = None
    parking = None
    neighborhood_name = None
    date_list = None
    date_sold = None
    term = None
    price_sold = None
    last_ask_price = None
    last_ask_price_m2 = None
    photos: list[str] = field(default_factory=list)

