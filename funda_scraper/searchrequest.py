from collections import OrderedDict
from typing import List, Optional

class SearchRequest(object):

    def __init__(
        self,
        area: str,
        want_to: str,
        page_start: int = 1,
        n_pages: int = 1,
        find_past: bool = False,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        days_since: Optional[int] = None,
        property_type: Optional[str] = None,
        min_floor_area: Optional[str] = None,
        max_floor_area: Optional[str] = None,
        sort: Optional[str] = None,
    ):
        """

        :param area: The area to search for properties, formatted for URL compatibility.
        :param want_to: Specifies whether the user wants to buy or rent properties.
        :param page_start: The starting page number for the search.
        :param n_pages: The number of pages to scrape.
        :param find_past: Flag to indicate whether to find past listings.
        :param min_price: The minimum price for the property search.
        :param max_price: The maximum price for the property search.
        :param days_since: The maximum number of days since the listing was published.
        :param property_type: The type of property to search for.
        :param min_floor_area: The minimum floor area for the property search.
        :param max_floor_area: The maximum floor area for the property search.
        :param sort: The sorting criterion for the search results.
        """
        # Init attributes
        self.area = area.lower().replace(" ", "-")
        self.property_type = property_type
        self.want_to = want_to
        self.find_past = find_past
        self.page_start = max(page_start, 1)
        self.n_pages = max(n_pages, 1)
        self.page_end = self.page_start + self.n_pages - 1
        self.min_price = min_price
        self.max_price = max_price
        self.days_since = days_since
        self.min_floor_area = min_floor_area
        self.max_floor_area = max_floor_area
        self.sort = sort

    def __repr__(self):
        return (
            f"FundaScraper(area={self.area}, "
            f"want_to={self.want_to}, "
            f"n_pages={self.n_pages}, "
            f"page_start={self.page_start}, "
            f"find_past={self.find_past}, "
            f"min_price={self.min_price}, "
            f"max_price={self.max_price}, "
            f"days_since={self.days_since}, "
            f"min_floor_area={self.min_floor_area}, "
            f"max_floor_area={self.max_floor_area}, "
            f"find_past={self.find_past})"
            f"min_price={self.min_price})"
            f"max_price={self.max_price})"
            f"days_since={self.days_since})"
            f"sort={self.sort})"
        )

    @property
    def to_buy(self) -> bool:
        """Determines if the search is for buying or renting properties."""
        if self.want_to.lower() in ["buy", "koop", "b", "k"]:
            return True
        elif self.want_to.lower() in ["rent", "huur", "r", "h"]:
            return False
        else:
            raise ValueError("'want_to' must be either 'buy' or 'rent'.")

    @property
    def check_days_since(self) -> int:
        """Validates the 'days_since' attribute."""
        if self.find_past:
            raise ValueError("'days_since' can only be specified when find_past=False.")

        if self.days_since in [None, 1, 3, 5, 10, 30]:
            return self.days_since
        else:
            raise ValueError("'days_since' must be either None, 1, 3, 5, 10 or 30.")

    @property
    def sort_by(self) -> str:
        """Validates the 'sort' attribute."""
        if self.sort in [
            None,
            "relevancy",
            "date_down",
            "date_up",
            "price_up",
            "price_down",
            "floor_area_down",
            "plot_area_down",
            "city_up",
            "postal_code_up"
        ]:
            return self.sort
        else:
            return None

    def reset(
        self,
        area: Optional[str] = None,
        property_type: Optional[str] = None,
        want_to: Optional[str] = None,
        page_start: Optional[int] = None,
        n_pages: Optional[int] = None,
        find_past: Optional[bool] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        days_since: Optional[int] = None,
        min_floor_area: Optional[str] = None,
        max_floor_area: Optional[str] = None,
        sort: Optional[str] = None,
    ) -> None:
        """Resets or initializes the search parameters."""
        if area is not None:
            self.area = area
        if property_type is not None:
            self.property_type = property_type
        if want_to is not None:
            self.want_to = want_to
        if page_start is not None:
            self.page_start = max(page_start, 1)
        if n_pages is not None:
            self.n_pages = max(n_pages, 1)
        if find_past is not None:
            self.find_past = find_past
        if min_price is not None:
            self.min_price = min_price
        if max_price is not None:
            self.max_price = max_price
        if days_since is not None:
            self.days_since = days_since
        if min_floor_area is not None:
            self.min_floor_area = min_floor_area
        if max_floor_area is not None:
            self.max_floor_area = max_floor_area
        if sort is not None:
            self.sort = sort