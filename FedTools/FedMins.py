# -*- coding: utf-8 -*-

from __future__ import print_function
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import pandas as pd
import pickle
import threading
import sys


class FederalReserveMins (object):
  '''
  This class firstly extracts the release date of each previous Federal Reserve 
  Minutes release. Subsequently, the class extracts the Federal Reserve Minutes,
  placing results into a DataFrame, indexed by release date.

  -------------------------------Arguments------------------------------------------- 
  
  main_url: Federal Reserve Open Monetary Policy (FOMC) website URL. (str)

  calendar_url: URL containing a list of FOMC Meeting dates. (str)

  historical_split: year considered as historical (historical vs current archive list). (int) 

  verbose: boolean determining printing during scraping. (bool)

  thread_num: the number of threads to use for web scraping  

  -------------------------------Returns--------------------------------------------- 

  dataset - a DataFrame containing meeting minutes, indexed by meeting date.

  '''

  def __init__(self,
               main_url = 'https://www.federalreserve.gov',
               calendar_url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm',
               historical_split = 2014,
               verbose = True,
               thread_num = 10
               ):
      
      self.main_url = main_url
      self.calendar_url = calendar_url
      self.HISTORICAL_SPLIT = historical_split
      self.verbose = verbose
      self.THREAD_NUM = thread_num
      self.dataset = None
      self.links = None
      self.full_links =None
      self.dates = None
      self.articles = None


  def _obtain_links(self, start_year): 
    '''
    internal function which constructs the links of all FOMC meetings, 
    beggining at 'start_year' and ending at current year.

    '''

    if self.verbose:
      print("Constructing Links.")
    
    self.links = []
    self.full_links = []

    fed_minutes_socket = urlopen(self.calendar_url)
    soup = BeautifulSoup(fed_minutes_socket, 'html.parser')

    fed_mins = soup.find_all('a', href = re.compile('^/monetarypolicy/fomcminutes\d{8}.htm'))
    self.links = [fed_min.attrs['href'] for fed_min in fed_mins]

    if start_year <= self.HISTORICAL_SPLIT:
      for year in range(start_year, self.HISTORICAL_SPLIT +1):
        fed_mins_annual_url = self.main_url + '/monetarypolicy/fomchistorical'+ str(year) +'.htm'
        fed_mins_annual_socket = urlopen(fed_mins_annual_url)
        fed_mins_annual_soup = BeautifulSoup(fed_mins_annual_socket, 'html.parser')
        historical_mins = fed_mins_annual_soup.findAll('a', text = 'HTML')
        historical_mins.extend(fed_mins_annual_soup.findAll('a', text = 'Minutes'))
        for historical_min in historical_mins:
          self.full_links.append(historical_min.attrs['href'])
          # catch all instances:
          annual_links = [x for x in self.full_links if 'fomcminutes' in x]
          annual_links.extend([x for x in self.full_links if 'MINUTES' in x])
          annual_links.extend([x for x in self.full_links if 'minutes' in x])
    
    # now find unique values:
    for x in annual_links:
      if x not in self.links:
        self.links.append(x)


  def _find_date_from_link(self, link):
    '''
    internal function which finds the meeting date within the relevant link.
    '''
    date = re.findall('[0-9]{8}', link)[0]

    if date[4] == '0':
      date = "{}/{}/{}".format(date[:4], date[5:6], date[6:])
    else:
      date = "{}/{}/{}".format(date[:4], date[4:6], date[6:])

    return date


  def _add_article(self, link, index = None):
    '''
    internal function which adds the related minutes for 1 link to the instance variable.
    'index' is the index in the article to add to. Due to multithreading, articles must be
    stored in the correct order.

    '''
    # write a dot as progress report:
    if self.verbose:
      sys.stdout.write(".")
      sys.stdout.flush()
    
    # append date of article content appropriately:
    self.dates.append(self._find_date_from_link(link))

    # depending on length of links, which link to open:
    if len(link) <= 50:
      fed_mins_output_socket = urlopen(self.main_url + link)
      fed_mins_output = BeautifulSoup(fed_mins_output_socket, 'html.parser')
      paragraph_delimiter = fed_mins_output.findAll('p')
      self.articles[index]= "\n\n".join([paragraph.get_text().strip() for paragraph in paragraph_delimiter])
    else:
      fed_mins_output_socket = urlopen(link)
      fed_mins_output = BeautifulSoup(fed_mins_output_socket, 'html.parser')
      paragraph_delimiter = fed_mins_output.findAll('p')
      self.articles[index]= "\n\n".join([paragraph.get_text().strip() for paragraph in paragraph_delimiter])


  def _multithreaded_article_retrieval(self):
    '''
    internal function which returns all articles. The function utilises multithreading to
    reduce scraping time.
    '''
    if self.verbose:
      print("Retrieving articles.")

    self.dates, self.articles = [], ['']*len(self.links)
    jobs = []
    # initiate process, start threads:
    index = 0
    while index < len(self.links):
      if len(jobs) < self.THREAD_NUM:
        thread = threading.Thread(target = self._add_article, args = (self.links[index], index,))
        jobs.append(thread)
        thread.start()
        index += 1
      else:
        thread = jobs.pop(0)
        thread.join()

    for thread in jobs:
      thread.join()

    for row in range(len(self.articles)):
      self.articles[row] = self.articles[row].strip()


  def find_minutes(self, start_year = 1993):
    '''
    This function returns federal reserve minutes within a pandas dataframe, 
    where the index corresponds to the release date.

    -------------------------------Arguments------------------------------------------ 
  
    start_year: The year which the user wishes to begin parsing from. If start_year
    is within the last 5 years, this value is ignored.

    -------------------------------Returns-------------------------------------------- 

    dataset - a DataFrame containing meeting minutes, indexed by meeting date.

    '''

    self._obtain_links(start_year)
    if self.verbose:
      print("Extracting Federal Reserve Minutes.")
    self._multithreaded_article_retrieval()

    self.dataset = pd.DataFrame(self.articles, index = pd.to_datetime(self.dates)).sort_index()
    self.dataset.columns = ['Federal_Reserve_Mins']
    self.dataset = self.dataset[~self.dataset.index.duplicated(keep='first')]

    for i in range(len(self.dataset)):
      self.dataset.iloc[i, 0] = self.dataset.iloc[i, 0].replace('\n', ' ')
      self.dataset.iloc[i, 0] = self.dataset.iloc[i, 0].replace('\r', ' ')
      self.dataset.iloc[i, 0] = self.dataset.iloc[i, 0].replace('\t', '')
      self.dataset.iloc[i, 0] = self.dataset.iloc[i, 0].replace('\xa0', '')
      self.dataset.iloc[i, 0] = self.dataset.iloc[i, 0].strip()

    return self.dataset


  def pickle_data(self, directory):
    '''
    This function pickles and stores the scraped dataset in a predefined directory.

    -------------------------------Arguments------------------------------------------ 
  
    directory: The filepath where the pickled dataset should be stored.

    -------------------------------Returns-------------------------------------------- 

    A pickled dataframe stored within the predefined directory.

    '''
    if directory:
      if self.verbose:
        print("Pickling Data, writing to {}".format(directory))
      with open(directory, "wb") as output:
        pickle.dump(self.dataset, output)



if __name__ == '__main__':
  dataset = FederalReserveMins().find_minutes()
  FederalReserveMins().pickle_data("DIRECTORY.pkl")