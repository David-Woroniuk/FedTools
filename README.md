# FedTools

An open source Python library for the scraping of Federal Reserve data.

By default, all modules within FedTools use 10 threads to increase scraping speed. By default, the Output is a 
Pandas DataFrame, indexed by release date of the materials. Additional serialised storage methods are optional.

## Installation

From Command Line:
```
$ python FedMinutes.py
```
Saves a Pickled Pandas DataFrame 'dataset.pkl', which contains all Meeting Minutes within search range, indexed by Date.

From Python:
```
pip install FedTools
from FedTools import MonetaryPolicyCommittee
```

## Usage

From Command Line:
```
$ python FedMinutes.py
```
Saves a Pickled Pandas DataFrame 'dataset.pkl', which contains all Meeting Minutes within search range, indexed by Date.

From Python:
```
pip install FedTools
from FedTools import MonetaryPolicyCommittee
dataset = MonetaryPolicyCommittee().find_statements()

MonetaryPolicyCommittee().pickle_data("DIRECTORY")
```
Returns a Pandas DataFrame 'dataset', which contains all Meeting Minutes, indexed by Date and a '.pkl' file saved within "DIRECTORY".

To edit input default arguments:
```sh
monetary_policy = MonetaryPolicyCommittee(
            main_url = 'https://www.federalreserve.gov', 
            calendar_url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm',
            historical_split = 2014,
            verbose = True,
            max_threads = 10)
            
dataset = monetary_policy.find_statements()

# serialise, save to "DIRECTORY":
monetary_policy.pickle_data("DIRECTORY")
```

All parameters above are optional, with a short explanation of each parameter outlined below:

| Argument | Description |
| ------ | --------- |
| main_url | Federal Reserve Open Monetary Policy (FOMC) website URL. (str) |
| calendar_url | URL containing a list of FOMC Meeting dates. (str) |
| historical_split | year(s) considered as historical ([check here][hist]). (int)  |
| verbose | boolean determining printing during scraping. (bool) |
| thread_num | the number of threads to use for web scraping. (int)   |


[hist]: <https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm>
