import pytest

from funda_scraper.preprocess import preprocess_data
from funda_scraper.scrape import FundaScraper


class TestFundaScraper(object):
    @pytest.fixture
    def scraper(self):
        return FundaScraper(
            area="amsterdam",
            want_to="buy",
            page_start=1,
            number_of_pages=1,
            find_past=False,
            min_price=100000,
            max_price=500000,
            days_since=None,
            property_type=None,
            min_floor_area=None,
            max_floor_area=None,
            sort=None,
        )

    def test_to_buy(self, scraper):
        assert scraper.to_buy is True

    def test_check_days_since(self, scraper):
        scraper.days_since = 5
        assert scraper.check_days_since == 5

    def test_check_sort(self, scraper):
        scraper.sort = "price_down"
        assert scraper.check_sort == "price_down"

    def test_reset(self, scraper):
        scraper.reset(area="rotterdam", number_of_pages=2)
        assert scraper.area == "rotterdam"
        assert scraper.number_of_pages == 2

    def test_fix_link(self, scraper):
        link = "https://www.funda.nl/detail/koop/den-haag/appartement-address-333/88888888/"
        fixed_link = scraper.fix_link(link)
        assert (
            "https://www.funda.nl/koop/den-haag/appartement-88888888-address-333/?old_ldp=true"
            == fixed_link
        )


def test_rent():
    scraper = FundaScraper(
        area="amsterdam", want_to="rent", find_past=False, page_start=1, number_of_pages=1
    )
    df = scraper.run(raw_data=True)
    assert len(scraper.links) == 15
    assert df.shape == (15, 27)
    assert df["city"].unique()[0] == "amsterdam"

    df = preprocess_data(df, is_past=False)
    assert df.shape[0] > 12
    assert df.shape[1] == 18


def test_rent_past():
    scraper = FundaScraper(
        area="amsterdam", want_to="rent", find_past=True, page_start=1, number_of_pages=1
    )
    df = scraper.run(raw_data=True)
    assert len(scraper.links) == 15
    assert df.shape == (15, 30)
    assert df["city"].unique()[0] == "amsterdam"

    df = preprocess_data(df, is_past=True)
    assert df.shape[0] > 12
    assert df.shape[1] == 21


def test_buy():
    scraper = FundaScraper(
        area="amsterdam", want_to="buy", find_past=False, page_start=1, number_of_pages=1
    )
    df = scraper.run(raw_data=True)
    assert len(scraper.links) == 15
    assert df.shape == (15, 27)
    assert df["city"].unique()[0] == "amsterdam"

    df = preprocess_data(df, is_past=False)
    assert df.shape[0] > 12
    assert df.shape[1] == 18


def test_buy_past():
    scraper = FundaScraper(
        area="amsterdam", want_to="buy", find_past=True, page_start=1, number_of_pages=1
    )
    df = scraper.run(raw_data=True)
    assert len(scraper.links) == 15
    assert df.shape == (15, 30)
    assert df["city"].unique()[0] == "amsterdam"

    df = preprocess_data(df, is_past=True)
    assert df.shape[0] > 12
    assert df.shape[1] == 21


def test_buy_house():
    scraper = FundaScraper(
        area="amsterdam",
        property_type="house",
        want_to="buy",
        find_past=False,
        page_start=1,
        number_of_pages=1,
    )
    df = scraper.run(raw_data=True)
    assert len(scraper.links) == 15
    assert df.shape == (15, 27)
    assert df["city"].unique()[0] == "amsterdam"

    df = preprocess_data(df, is_past=False)
    assert df.shape[0] > 12
    assert df.shape[1] == 18
    assert df["house_type"].unique()[0] == "huis"


def test_buy_apartment():
    scraper = FundaScraper(
        area="amsterdam",
        property_type="apartment",
        want_to="buy",
        find_past=False,
        page_start=1,
        number_of_pages=1,
    )
    df = scraper.run(raw_data=True)
    assert len(scraper.links) == 15
    assert df.shape == (15, 27)
    assert df["city"].unique()[0] == "amsterdam"

    df = preprocess_data(df, is_past=False)
    assert df.shape[0] > 12
    assert df.shape[1] == 18
    assert df["house_type"].unique()[0] == "appartement"


def test_buy_mixed():
    scraper = FundaScraper(
        area="amsterdam",
        property_type="apartment,house",
        want_to="buy",
        find_past=False,
        page_start=1,
        number_of_pages=1,
    )
    df = scraper.run(raw_data=True)
    assert len(scraper.links) == 15
    assert df.shape == (15, 27)
    assert df["city"].unique()[0] == "amsterdam"

    df = preprocess_data(df, is_past=False)
    assert df.shape[0] > 12
    assert df.shape[1] == 18
    assert set(df["house_type"].unique()) == set(["appartement", "huis"])
