# -*- coding: utf-8 -*-

from __future__ import print_function
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from fake_useragent import UserAgent
from datetime import datetime
import re
import pandas as pd
import pickle
import threading
import sys
import os


class BeigeBooks(object):
    """
    This class firstly checks that the input arguments are of the correct type, followed
    by extracting the release month of each previous Beige Book. Subsequently, the Beige Book
    data is extracted, placing the results into a Pandas DataFrame.
    :param main_url: the Federal Reserve Open Monetary Policy (FOMC) website URL. (str)
    :param beige_book_url: the Beige Book directory page. (str)
    :param start_year: the year which the user wishes to begin parsing from. (int)
    :param historical_split: the year considered as historical (historical vs current archive list). (int)
    :param verbose: boolean determining printing during scraping. (bool)
    :param thread_num: the number of threads to use for web scraping. (int)
    :return: dataset: a DataFrame containing meeting minutes, indexed by meeting date. (pd.DataFrame)
    """

    def __init__(self,
                 main_url: str = 'https://www.federalreserve.gov',
                 beige_book_url: str = 'https://www.federalreserve.gov/monetarypolicy/beige-book-default.htm',
                 start_year: int = 1996,
                 historical_split: int = 2022,
                 verbose: bool = True,
                 thread_num: int = 10
                 ):

        if not all(isinstance(v, str) for v in [main_url, beige_book_url]):
            raise TypeError("The 'main_url' and 'beige_book_url' arguments must be string types.")
        if not all(isinstance(v, int) for v in [start_year, historical_split, thread_num]):
            raise TypeError("The 'start_year', 'historical_split' and 'thread_num' arguments must be integer types.")
        if not isinstance(verbose, bool):
            raise TypeError("The 'verbose' argument must be a boolean type.")

        self.main_url = main_url
        self.beige_book_url = beige_book_url
        self.start_year = start_year
        self.HISTORICAL_SPLIT = historical_split
        self.verbose = verbose
        self.THREAD_NUM = thread_num
        self.dataset = None
        self.links = None
        self.dates = None
        self.articles = None

    def _obtain_links(self, start_year: int):
        """
        The helper function constructs the links of all FOMC meetings,
        beginning at the start_year' argument, and continuing to the current
        date.
        :param start_year: the year at which the link construction begins. (int)
        """
        if not isinstance(start_year, int):
            raise TypeError("The 'start_year' variable must be an integer type.")

        if self.verbose:
            print("Constructing links between {} and {}".format(start_year, datetime.today().year))

        self.links = []
        beige_book_socket = self._urlopen_with_ua(self.beige_book_url)
        soup = BeautifulSoup(beige_book_socket, 'html.parser')
        beige_books = soup.find_all('a', href=re.compile('^/monetarypolicy/beigebook\\d{6}.htm'))
        beige_books += soup.find_all('a', href=re.compile('^/monetarypolicy/beigebook\\d{8}.htm'))
        self.links = [beige_book.attrs['href'] for beige_book in beige_books]

        if start_year <= self.HISTORICAL_SPLIT:
            for year in range(start_year, self.HISTORICAL_SPLIT + 1):
                beige_book_annual_url = self.main_url + '/monetarypolicy/beigebook' + str(year) + '.htm'
                beige_book_annual_socket = self._urlopen_with_ua(beige_book_annual_url)
                bb_annual_soup = BeautifulSoup(beige_book_annual_socket, 'html.parser')
                historical_statements = bb_annual_soup.findAll('a', string = re.compile('^HTML'))
                for historical_statement in historical_statements:
                    self.links.append(historical_statement.attrs['href'])

    @staticmethod
    def _urlopen_with_ua(url: str) -> str:
        """
        This helper function adds user agent credentials to the
        request, enabling the script to interact with the Federal
        Reserve website.
        :param url: the url to be queried, without a user agent. (str)
        :return: urlopen(req): the url opened using a user agent. (str)
        """
        if not isinstance(url, str):
            raise TypeError("The 'url' argument must be a string type.")

        ua = UserAgent()
        req = Request(url)
        req.add_header("user-agent", ua.chrome)
        return urlopen(req)

    @staticmethod
    def _find_date_from_link(link: str) -> str:
        """
        This helper function determines the FOMC meeting date from the relevant link.
        The function firstly checks that the link is a string type, followed by parsing
        the string to generate the date. The date string is subsequently returned.
        :param link: the link string to be parsed for dates. (str)
        :return: date: the date string parsed from the link string. (str)
        """
        if not isinstance(link, str):
            raise TypeError("The 'link' argument must be a string type.")

        date = re.findall('[0-9]{6}', link)[0]
        if date[4] == '0':
            date = "{}/{}/{}".format(date[:4], date[5:6], "01")
        else:
            date = "{}/{}/{}".format(date[:4], date[4:6], "01")
        return date

    def _add_article(self, link: str, index: int = None):
        """
        This helper function adds the related minutes for 1 link to the instance variable.
        Multithreading stipulates that the articles must be stored in the correct order, where
        the 'index' argument is the index in the article to add to.
        :param link: the link to be opened and data generated for. (str)
        :param index: the index associated with the link. (int)
        """
        if not isinstance(link, str):
            raise TypeError("The 'link' argument must be a string type.")
        if not isinstance(index, (type(None), int)):
            raise TypeError("The 'index' argument must either be a None type or a integer type.")

        if self.verbose:
            sys.stdout.write(".")
            sys.stdout.flush()

        self.dates.append(self._find_date_from_link(link))
        if not self.main_url in link:
            beige_book_output_socket = self._urlopen_with_ua(self.main_url + link)
            beige_book_output = BeautifulSoup(beige_book_output_socket, 'html.parser')
            paragraph_delimiter = beige_book_output.findAll('p')
            self.articles[index] = "\n\n".join([paragraph.get_text().strip() for paragraph in paragraph_delimiter])
        else:
            beige_book_output_socket = self._urlopen_with_ua(link)
            beige_book_output = BeautifulSoup(beige_book_output_socket, 'html.parser')
            paragraph_delimiter = beige_book_output.findAll('p')
            self.articles[index] = "\n\n".join([paragraph.get_text().strip() for paragraph in paragraph_delimiter])

    def _multithreaded_article_retrieval(self):
        """
        This helper function returns all articles associated with each link. The function firstly
        initiates the threads, as specified by the 'thread_num' argument passed to the class. The
        function uses each thread to efficiently extract the articles and store the outcome.
        """
        if self.verbose:
            print("Retrieving articles.")

        self.dates, self.articles = [], [''] * len(self.links)
        jobs = []
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

    def find_beige_books(self):
        """
        This function acts as the main public function of the class, returning the Beige Books
        by efficiently extracting the information from the FOMC Website. The function then places each
        Beige Book into a Pandas DataFrame, indexed by the meeting date string.
        :return: dataset: a Pandas DataFrame containing the meeting minutes, indexed by meeting date. (pd.DataFrame)
        """
        self._obtain_links(self.start_year)
        if self.verbose:
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

    def pickle_data(self, directory: str) -> bool:
        """
        This public function acts as a main public function, for extraction and pickling of
        the extracted data to the 'directory' argument passed. The function checks that the
        directory argument is a string type, followed by checking if the directory ends with
        the appropriate extension. The folder is then created if necessary, followed by the
        data being written to the pickle file, where a boolean is returned to denote success / failure
        of the file write operation.
        :param directory: the directory to which the file should be written. (str)
        :return: bool: determines if the file was written correctly. (bool)
        """
        if not isinstance(directory, str):
            raise TypeError("The 'directory' argument must be a string type.")
        if not directory.endswith((".pkl", ".pickle")):
            raise TypeError("The pickle file directory should end with a '.pkl' or '.pickle' extension.")
        if not os.path.exists(os.path.split(directory)[0]):
            if self.verbose:
                print("Creating {} directory.".format(os.path.split(directory)[0]))
            os.mkdir(os.path.split(directory)[0])
        output_dataset = self.find_beige_books()
        try:
            with open(directory, "wb") as pickle_output:
                pickle.dump(output_dataset, pickle_output)
            return True
        except(NotImplementedError, FileNotFoundError):
            return False


if __name__ == '__main__':
    dataset = BeigeBooks().find_beige_books()