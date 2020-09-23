# FedTools

An open source Python library for the scraping of Federal Reserve data.

By default, all modules within FedTools use 10 threads to increase scraping speed. By default, the Output is a 
Pandas DataFrame, indexed by release date of the materials. Additional serialised storage methods are optional.

## Installation

From Python:
```
pip install FedTools

from FedTools import MonetaryPolicyCommittee
from FedTools import BeigeBooks
from FedTools import FederalReserveMins
```

## Usage

Returns a Pandas DataFrame 'dataset', which contains all Meeting Minutes, indexed by Date and a '.pkl' file saved within "DIRECTORY":
```
pip install FedTools
from FedTools import MonetaryPolicyCommittee
dataset = MonetaryPolicyCommittee().find_statements()

MonetaryPolicyCommittee().pickle_data("DIRECTORY")
```

Returns a Pandas DataFrame 'dataset', which contains all Beige Books, indexed by Date and a '.pkl' file saved within "DIRECTORY":
```
pip install FedTools
from FedTools import BeigeBooks
dataset = BeigeBooks().find_beige_books()

BeigeBooks().pickle_data("DIRECTORY")
```

Returns a Pandas DataFrame 'dataset', which contains all Federal Reserve Minutes since 1993, indexed by Date and a '.pkl' file saved within "DIRECTORY":
```
pip install FedTools
from FedTools import FederalReserveMins
dataset = FederalReserveMins().find_minutes()

FederalReserveMins().pickle_data("DIRECTORY")
```

## Edit Default Input Arguments
```sh
monetary_policy = MonetaryPolicyCommittee(
            main_url = 'https://www.federalreserve.gov', 
            calendar_url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm',
            historical_split = 2014,
            verbose = True,
            thread_num = 10)
            
dataset = monetary_policy.find_statements()

# serialise, save to "DIRECTORY":
monetary_policy.pickle_data("DIRECTORY")

-------------------------------------------------------------------------------------------------------------------

beige_books = BeigeBooks(
            main_url = 'https://www.federalreserve.gov', 
            beige_book_url='https://www.federalreserve.gov/monetarypolicy/beige-book-default.htm',
            historical_split = 2019,
            verbose = True,
            thread_num = 10)
            
            
dataset = beige_books.find_beige_books()

# serialise, save to "DIRECTORY":
beige_books.pickle_data("DIRECTORY")

-------------------------------------------------------------------------------------------------------------------

fed_mins = FederalReserveMins(
            main_url = 'https://www.federalreserve.gov', 
            calendar_url ='https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm',
            historical_split = 2014,
            verbose = True,
            thread_num = 10)
          
dataset = fed_mins.find_minutes()

# serialise, save to "DIRECTORY":
fed_mins.pickle_data("DIRECTORY")
```

All parameters above are optional, with a short explanation of each parameter outlined below:

| Argument | Description |
| ------ | --------- |
| main_url | Federal Reserve Open Monetary Policy (FOMC) website URL. (str) |
| calendar_url | URL containing a list of FOMC Meeting dates and Minutes release dates. (str) |
| beige_book_url | URL containing a list of Beige Book release dates. (str)
| historical_split | first year considered as historical ([Check Here for FOMC and Minutes][hist] or [Check Here for Beige Books][hist1]). (int)  |
| verbose | boolean determining printing during scraping. (bool) |
| thread_num | the number of threads to use for web scraping. (int)   |






[hist]: <https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm>
[hist1]: <https://www.federalreserve.gov/monetarypolicy/beige-book-archive.htm>

