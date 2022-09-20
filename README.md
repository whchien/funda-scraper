# FundaScraper

`FundaScaper` provides you the easiest way to perform web scraping from Funda, the Dutch housing website. 
You can find listings from either house-buyer or rental market, and you can find historical data from the past few year.


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


