# -*- coding: utf-8 -*-

from __future__ import print_function
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import pandas as pd
import pickle
import threading
import sys


class BeigeBooks(object):
  '''
  This class firstly extracts the release month of each previous Beige Book.
  Subsequently, the class extracts the Beige Book, placing results into a 
  DataFrame, indexed by release date.

  -------------------------------Arguments------------------------------------------- 

  main_url: Federal Reserve Open Monetary Policy (FOMC) website URL. (str)

  beige_book_url: Beige Book directory page. (str)

  historical_split: year considered as historical (historical vs current archive list). (int) 

  verbose: boolean determining printing during scraping. (bool)

  thread_num: the number of threads to use for web scraping  

  -------------------------------Returns--------------------------------------------- 

  dataset - a DataFrame containing Beige Books, indexed by release date.

  '''

  def __init__(self,
               main_url='https://www.federalreserve.gov',
               beige_book_url='https://www.federalreserve.gov/monetarypolicy/beige-book-default.htm',
               historical_split=2019,
               verbose=True,
               thread_num=10
               ):

    self.main_url = main_url
    self.beige_book_url = beige_book_url
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
    Beige_Book_Socket = urlopen(self.beige_book_url)
    soup = BeautifulSoup(Beige_Book_Socket, 'html.parser')

    Beige_Books = soup.find_all('a', href=re.compile('^/monetarypolicy/beigebook\d{6}.htm'))
    self.links = [Beige_Book.attrs['href'] for Beige_Book in Beige_Books]

    if start_year <= self.HISTORICAL_SPLIT:
      for year in range(start_year, self.HISTORICAL_SPLIT + 1):
        beige_book_annual_url = self.main_url + '/monetarypolicy/beigebook' + str(year) + '.htm'
        beige_book_annual_socket = urlopen(beige_book_annual_url)
        bb_annual_soup = BeautifulSoup(beige_book_annual_socket, 'html.parser')
        historical_statements = bb_annual_soup.findAll('a', text='HTML')
        for historical_statement in historical_statements:
          self.links.append(historical_statement.attrs['href'])

  def _find_date_from_link(self, link):
    '''
    internal function which finds the meeting date within the relevant link.
    '''
    date = re.findall('[0-9]{6}', link)[0]
    if date[4] == '0':
      date = "{}/{}/{}".format(date[:4], date[5:6], '01')
    else:
      date = "{}/{}/{}".format(date[:4], date[4:6], '01')

    return date

  def _add_article(self, link, index=None):
    '''
    internal function which adds the related article for 1 link to the instance variable.
    'index' is the index in the article to add to. Due to multithreading, articles must be
    stored in the correct order.

    '''
    # write a dot as progress report:
    if self.verbose:
      sys.stdout.write(".")
      sys.stdout.flush()

    # append date of article content appropriately:
    self.dates.append(self._find_date_from_link(link))

    # conditional scoekts depending on length of links:
    if len(link) <= 35:
      Beige_Book_Output_socket = urlopen(self.main_url + link)
      Beige_Book_Output = BeautifulSoup(Beige_Book_Output_socket, 'html.parser')
      paragraph_delimiter = Beige_Book_Output.findAll('p')
      self.articles[index] = "\n\n".join([paragraph.get_text().strip() for paragraph in paragraph_delimiter])
    else:
      Beige_Book_Output_socket = urlopen(link)
      Beige_Book_Output = BeautifulSoup(Beige_Book_Output_socket, 'html.parser')
      paragraph_delimiter = Beige_Book_Output.findAll('p')
      self.articles[index] = "\n\n".join([paragraph.get_text().strip() for paragraph in paragraph_delimiter])

  def _multithreaded_article_retrieval(self):
    '''
    internal function which returns all articles. The function utilises multithreading to
    reduce scraping time.
    '''

    if self.verbose:
      print("Retrieving articles.")

    self.dates, self.articles = [], [''] * len(self.links)
    jobs = []
    # initiate process, start threads:
    index = 0
    while index < len(self.links):
      if len(jobs) < self.THREAD_NUM:
        thread = threading.Thread(target=self._add_article, args=(self.links[index], index,))
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

  def find_beige_books(self, start_year=1996):
    '''
    This function returns beige book information within a pandas dataframe, 
    where the index corresponds to the release date.

    -------------------------------Arguments------------------------------------------ 

    start_year: The year which the user wishes to begin parsing from. This year must be 
    greater than or equal to 1996 (start_year >= 1996).

    -------------------------------Returns-------------------------------------------- 

    dataset - a DataFrame containing Beige Books, indexed by release date.

    '''

    self._obtain_links(start_year)
    print("Extracting the past {} Beige Books.".format(len(self.links)))
    self._multithreaded_article_retrieval()

    self.dataset = pd.DataFrame(self.articles, index=pd.to_datetime(self.dates)).sort_index()
    self.dataset.columns = ['Beige_Book']

    if self.verbose:
      sys.stdout.write(".")
      sys.stdout.flush()

    for i in range(len(self.dataset)):
      self.dataset.iloc[i, 0] = self.dataset.iloc[i, 0].replace('\n', ' ')
      self.dataset.iloc[i, 0] = self.dataset.iloc[i, 0].replace('\r', ' ')
      self.dataset.iloc[i, 0] = self.dataset.iloc[i, 0].replace('\t', '')
      self.dataset.iloc[i, 0] = self.dataset.iloc[i, 0].replace('\xa0', '')

    return self.dataset

  def pickle_data(self, directory="../data/Beige_Books.pkl"):
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
  dataset = BeigeBooks().find_beige_books()

