import pandas as pd
import pytest

from funda_scraper.preprocess import (
    preprocess_data,
    clean_energy_label,
    clean_date_format,
    clean_price,
    clean_year,
    clean_living_area,
    find_n_room,
    find_n_bedroom,
    find_n_bathroom,
    map_dutch_month,
)
from datetime import datetime


def test_clean_price():
    assert clean_price("€ 1.000.000") == 1000000
    assert clean_price("Prijs op aanvraag") == 0
    assert clean_price(None) == 0


def test_clean_year():
    assert clean_year("1990") == 1990
    assert clean_year("1990-2000") == 1990
    assert clean_year("before 1900") == 1900
    assert clean_year("unknown") == 0


def test_clean_living_area():
    assert clean_living_area("100 m²") == 100
    assert clean_living_area("unknown") == 0
    assert clean_living_area(None) == 0


def test_find_n_room():
    assert find_n_room("5 kamers") == 5
    assert find_n_room("unknown") == 0
    assert find_n_room("3 rooms") == 3


def test_find_n_bedroom():
    assert find_n_bedroom("2 slaapkamers") == 2
    assert find_n_bedroom("unknown") == 0
    assert find_n_bedroom("4 bedrooms") == 4


def test_find_n_bathroom():
    assert find_n_bathroom("1 badkamer") == 1
    assert find_n_bathroom("unknown") == 0
    assert find_n_bathroom("3 bathrooms") == 3


def test_map_dutch_month():
    assert map_dutch_month("januari") == "January"
    assert map_dutch_month("oktober") == "October"
    assert map_dutch_month("unknown") == "unknown"


def test_clean_energy_label():
    assert clean_energy_label("A") == "A"
    assert clean_energy_label("A+") == ">A+"
    assert clean_energy_label("unknown") == "unknown"


def test_clean_date_format():
    assert isinstance(clean_date_format("10 januari 2020"), datetime)
    assert isinstance(clean_date_format("unknown"), str)
    assert isinstance(clean_date_format("2 weken geleden"), datetime)
    assert isinstance(clean_date_format("Vandaag"), datetime)


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
        "photo": ["dummy"],
    }
    return pd.DataFrame(data)


class TestPreprocessData:
    def test_is_past_true(self, input_data):
        result = preprocess_data(df=input_data, is_past=True)
        assert result.shape == (1, 21)
        assert result["house_type"].item() == "appartement"
        assert result["price"].item() == 500000
        assert result["room"].item() == 4
        assert result["bedroom"].item() == 3
        # assert df["term_days"].item() == 13
        assert result["energy_label"].item() == ">A+"

        assert not result.empty
        assert "price" in result.columns
        assert "living_area" in result.columns
        assert "room" in result.columns
        assert "bathroom" in result.columns
        assert "year_built" in result.columns
        assert "house_age" in result.columns
        assert "date_sold" in result.columns
