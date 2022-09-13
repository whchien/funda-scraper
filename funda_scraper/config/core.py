from diot import Diot
import yaml
from yaml.loader import SafeLoader

with open("funda_scraper/config/config.yaml") as f:
    data = yaml.load(f, Loader=SafeLoader)

config = Diot(data)
