# FundaScraper

[![Build Status](https://app.travis-ci.com/whchien/funda-scraper.svg?branch=main)](https://app.travis-ci.com/whchien/funda-scraper)

`FundaScaper` provides you the easiest way to perform web scraping from Funda, the Dutch housing website. 
You can find listings from either house-buyer or rental market, and you can find historical data from the past few year.

Please note:
1. Scraping this website is only allowed for personal use (as per Funda's Terms and Conditions).
2. Any commercial use of this Python package is prohibited. The author holds no liability for any misuse of the package.


## Install
```
pip install funda-scraper
```

## Quickstart 
```
from funda_scraper import FundaScraper

scraper = FundaScraper(area="amsterdam", want_to="rent", find_past=False)
df = scraper.run()
df.head()
```
![image](https://i.imgur.com/mmN9mjQ.png)

You can pass several arguments to `FundaScraper()` for customized scraping:
- `area`: Specify the city or specific area you want to look for, eg. Amsterdam, Utrecht, Rotterdam, etc
- `want_to`: You can choose either `buy` or `rent`, which finds houses either for sale or for rent. 
- `find_past`: Specify whether you want to check the historical data. The default is `False`.
- `n_pages`: Indicate how many pages you want to look up. The default is `1`. 


## Advanced usage

You can check the [example notebook](https://colab.research.google.com/drive/1hNzJJRWxD59lrbeDpfY1OUpBz0NktmfW?usp=sharing) for further details. 
Please give me a [star](https://github.com/whchien/funda-scraper) if you find this project helpful. 


