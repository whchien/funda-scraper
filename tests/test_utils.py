import pytest
from pathlib import Path
import shutil

from funda_scraper.utils import get_cookies

TEST_PATH = Path("funda_scraper").joinpath("test_secrets")


@pytest.fixture(scope="function")
def setup_teardown():
    if TEST_PATH.is_dir():
        shutil.rmtree(TEST_PATH)
    
    yield "Setup and Teardown..."

    if TEST_PATH.is_dir():
        shutil.rmtree(TEST_PATH)

@pytest.mark.skip(reason="Temporarily skipping this test for debugging")
def test_get_cookies(setup_teardown):
    print(setup_teardown)
    get_cookies(TEST_PATH)
    
    # Assert if the cookies.pkl file is created
    cookies_file = TEST_PATH.joinpath("cookies.pkl")
    assert cookies_file.exists(), "cookies.pkl not found in the test_secrets folder."
