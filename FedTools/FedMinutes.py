# -*- coding: utf-8 -*-

from __future__ import print_function
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import pandas as pd
import pickle
import threading
import sys


class MonetaryPolicyCommittee (object):
  '''
  This class firstly extracts the meeting dates of each previous FOMC meeting.
  Subsequently, the class extracts the meeting minutes of the FOMC meeting, placing 
  results into a DataFrame, indexed by meeting date.

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
               thread_num = 40
               ):
      
      self.main_url = main_url
      self.calendar_url = calendar_url
      self.HISTORICAL_SPLIT = historical_split
      self.verbose = verbose
      self.THREAD_NUM = thread_num
      self.dataset = None
      self.links = None
      self.dates = None
      self.articles = None


  def _obtain_links(self, start_year):
    '''
    internal function which constructs the links of all FOMC meetings, 
    beggining at 'start_year' and ending at current year.

    '''

    if self.verbose:
      print("Constructing links.")

    self.links = []
    federal_reserve_meetings_socket = urlopen(self.calendar_url)
    soup = BeautifulSoup(federal_reserve_meetings_socket, 'html.parser')

    policy_statements = soup.find_all('a', href = re.compile('^/newsevents/pressreleases/monetary\d{8}a.htm'))
    self.links = [policy_statement.attrs['href'] for policy_statement in policy_statements]

    if start_year <= self.HISTORICAL_SPLIT:        
      for year in range(start_year, self.HISTORICAL_SPLIT + 1):
        fomc_annual_url = self.main_url + '/monetarypolicy/fomchistorical' + str(year) + '.htm'
        fomc_annual_socket = urlopen(fomc_annual_url)
        annual_soup = BeautifulSoup(fomc_annual_socket, 'html.parser')
        historical_statements = annual_soup.findAll('a', text = 'Statement')
        for historical_statement in historical_statements:
          self.links.append(historical_statement.attrs['href'])
  

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

    
  def _add_article(self, link, index=None):
    '''
    internal function which adds the related article for 1 link to the instance variable.
    'index' is the index in the article to add to. Due to multithreading, articles must be
    stored in the correct order.
    '''

    if self.verbose:
      sys.stdout.write(".")
      sys.stdout.flush()

    # date of the article content:
    self.dates.append(self._find_date_from_link(link))


    policy_statement_socket = urlopen(self.main_url + link)
    policy_statement = BeautifulSoup(policy_statement_socket, 'html.parser')
    paragraph_delimiter = policy_statement.findAll('p')
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


  def find_statements(self, start_year = 1990):
    '''
    This function returns federal reserve minutes within a pandas dataframe, 
    where the index corresponds to the meeting date.

    -------------------------------Arguments------------------------------------------ 
  
    start_year: The year which the user wishes to begin parsing from. If start_year
    is within the last 5 years, this value is ignored.

    -------------------------------Returns-------------------------------------------- 

    dataset - a DataFrame containing meeting minutes, indexed by meeting date.

    '''

    self._obtain_links(start_year)
    print("Extracting the past {} FOMC Statements.".format(len(self.links)))
    self._multithreaded_article_retrieval()

    self.dataset = pd.DataFrame(self.articles, index = pd.to_datetime(self.dates)).sort_index()
    self.dataset.columns = ['FOMC_Statements']
    return self.dataset


  def pickle_data(self, directory = "../data/minutes.pkl"):
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
  dataset = MonetaryPolicyCommittee().find_statements()
  MonetaryPolicyCommittee().pickle_data("./dataset.pkl")

