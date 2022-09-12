import argparse
import logging
import time

from scraper.utils import str2bool
from scraper.scrape import FundaScraper
from scraper.db import DBManager

logger = logging.getLogger(__name__)


def main_runner(args: argparse.Namespace) -> None:
    start = time.time()

    # Scrape results
    scraper = FundaScraper(
        to_buy=args.to_buy,
        area=args.area,
        n_pages=args.n_pages,
        find_past=args.find_past,
    )
    scraper.run()
    result = scraper.result_df

    # Clean data
    manager = DBManager("funda.db")
    manager.write_raw(result)
    manager.clean_raw()

    logger.info(f"Total time used:  {time.time() - start}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--to_buy",
        type=str2bool,
        default=True,
        help="If true, find houses for close. Else find houses for rent.",
    )
    parser.add_argument(
        "--area",
        default="amsterdam",
        type=str,
        help="Define the target area to look for houses.",
    )
    parser.add_argument(
        "--n_pages",
        type=int,
        default=1,
        help="Define how many pages to look for. "
        "If set 999, all results will be retrieved.",
    )
    parser.add_argument(
        "--find_past",
        type=str2bool,
        default=False,
        help="Define whether only look at historical data.",
    )

    args = parser.parse_args()
    main_runner(args)
