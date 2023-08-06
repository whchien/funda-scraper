import pandas as pd
import pytest

from funda_scraper.preprocess import preprocess_data


@pytest.fixture
def input_data():
    data = {
        "url": ["https://www.funda.nl/koop/utrecht/appartement-00000000-dummy-100/"],
        "price": ["na"],
        "address": ["dummy 10"],
        "descrip": ["dummy"],
        "listed_since": ["Verkocht"],
        "zip_code": ["1111 AA"],
        "size": ["100 m²"],
        "year": ["2000"],
        "living_area": ["78 m²"],
        "kind_of_house": ["Eengezinswoning"],
        "building_type": ["Bestaande bouw"],
        "num_of_rooms": ["4 kamers (3 slaapkamers)"],
        "num_of_bathrooms": ["1 badkamer en 1 apart toilet"],
        "layout": ["Aantal kamers 4 kamers (3 slaapkamers)"],
        "energy_label": ["A++++"],
        "insulation": ["Dubbel glas"],
        "heating": ["dummy"],
        "ownership": ["dummy"],
        "exteriors": ["dummy"],
        "parking": ["dummy"],
        "neighborhood_name": ["dummy"],
        "date_list": ["30 juni 2023"],
        "date_sold": ["13 juli 2023"],
        "term": ["13 dagen"],
        "price_sold": ["€ 500.000 k.k."],
        "last_ask_price": ["€ 500.000 kosten koper"],
        "last_ask_price_m2": ["dummy"],
        "city": ["utrecht"],
        "log_id": ["dummy"],
    }
    return pd.DataFrame(data)


class TestPreprocessData:
    def test_is_past_true(self, input_data):
        df = preprocess_data(df=input_data, is_past=True)
        assert df.shape == (1, 23)
        assert df["house_type"].item() == "appartement"
        assert df["price"].item() == 500000
        assert df["room"].item() == 4
        assert df["bedroom"].item() == 3
        assert df["term_days"].item() == 13
        assert df["energy_label"].item() == ">A+"
