import scrapy  # pip install scrapy  --> scrapy ver. > 2.4 to use asyncio 
import logging
from datetime import datetime, timedelta


class transaction_spider(scrapy.Spider):
    """
        A spider class for collecting transactions from the EU ETS registry.

        This spider class uses the Scrapy framework to collect transactions from the EU ETS (European Union Emission Trading System) registry. It uses the asyncio and scrapy libraries to perform asynchronous requests and parse web pages. The class is configured to handle requests, responses, and perform the necessary actions to collect transaction data and store it in a CSV file.

    Attributes:

        name (str): The name of the spider.
        start_urls (str): The starting URL for collecting transactions.
        custom_settings (dict): A dictionary of custom parameters for spider configuration.
    Methods:

        start_requests(): A method for starting spider requests.
        parse_checker(response): A method for checking if the CSV file is up-to-date.
        parse_pages(response): A method for parsing transaction pages.
        parse(response): A method for parsing transactions on web pages.
    """
    name = "transaction_spider"

    start_urls = "https://ec.europa.eu/clima/ets/transaction.do?endDate=&suppTransactionType=-1&transactionStatus=4&originatingAccountType=-1&originatingAccountIdentifier=&originatingAccountHolder=&languageCode=en&destinationAccountIdentifier=&transactionID=&transactionType=-1&destinationAccountType=-1&search=Search&toCompletionDate=&originatingRegistry=-1&destinationAccountHolder=&fromCompletionDate=&destinationRegistry=-1&startDate=&TITLESORT-currentSortSettings-transactionDate-H=A&currentSortSettings=transactionDate%20ASC"

    custom_settings = {
        "LOG_LEVEL": "INFO",
    }

    def start_requests(self): 
        """Override the scrapy.Spider.start_requests method to use parse_checker instead of parse as callback function for the first request.

        Yields:
            scrapy.Request: the request to parse_checker method. 
        """
        yield scrapy.Request(self.start_urls, callback=self.parse_checker)

    # ------- Part to check if our CSV is up to date -------
    def parse_checker(self, response):
        """
            Check the latest transactions in the EU ETS registry.

            This function compares the date of the last update of the 'transaction_check.txt' file with the date of the latest transaction on the site, and takes actions accordingly.

        Args:
            response (scrapy.Response): The HTTP response of the page to be parsed.

        Yields:
            scrapy.Request: A follow-up request for the transaction page if certain conditions are met.
        """
        date_format = "%Y-%m-%d %H:%M:%S.%f"
        last_date = (
            response.css(
                "table#tblTransactionSearchResult tr:nth-child(3) td:nth-child(3) span::text"
            )
            .get(default="")
            .strip()
        )

        with open("../../transaction_check.txt", "r") as f:
            lines = f.readlines()
        date_verif = lines[3].split(" : ")[-1].strip()
        print("DATE", last_date, "==", date_verif, date_verif == last_date)

        with open("../../transaction_check.txt", "w") as f:
            f.write(
                f"Date from last launch of transaction_spider.py : {datetime.now()}\n"
            )
            if last_date != date_verif:
                f.write(
                    f"Date from last update of file transaction_check.txt : {datetime.now()}\n\n"
                )
                f.write(f"Date from last update of the website : {last_date}\n")
                print("File updated.")
            else:
                f.write("".join(lines[1:]))
                print("File already up to date.")

        delta = datetime.now() - datetime.strptime(
            lines[1].split(" : ")[-1].strip(), date_format
        )
        print("Delta between both dates : ", delta)
        # We put back the same condition since we don't want to start the scraping in the open mode
        if date_verif != last_date or delta >= timedelta(days=90):
            yield scrapy.Request(
                "https://ec.europa.eu/clima/ets/transaction.do?languageCode=en&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&search=Search&currentSortSettings=",
                callback=self.parse_pages,
            )

    # ----- SCRAPING PART ----- (only if our CSV is not up to date)
    async def parse_pages(self, response):
        """Parse of the transaction pages from the EU ETC registry.

        Args:
            response (scrapy.Response): The http response of the page to be parsed.

        Yields:
            scrapy.Request: A request to parse the next page of transactions.
        """
        pages = int(
            response.xpath("//input[@name='resultList.lastPageNumber']/@value").get()
        )
        for page in range(2, pages + 2):
            url = f"https://ec.europa.eu/clima/ets/transaction.do?languageCode=fr&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&backList=%3CBack&resultList.currentPageNumber={page}"
            yield response.follow(url, callback=self.parse)

    async def parse(self, response):
        """Extracts data from a table in the response.

        Args:
        response (scrapy.http.Response): The response object containing the HTML page to parse.

        Yields:
        dict: A dictionary containing the extracted data from each row in the table. The keys in the dictionary
        represent the different columns in the table, such as 'Transaction_ID', 'Transaction_Type', etc.
        """
        total_pages = response.xpath(
            "//input[@name='resultList.lastPageNumber']/@value"
        ).get()
        page = response.xpath(
            "//input[@name='resultList.currentPageNumber']/@value"
        ).get()
        print(f"page {page} out of {total_pages}")

        if total_pages is None:
            logging.warning(f"Page content is None for {response.url}, retrying...")
            yield response.follow(response.url, callback=self.parse)
        else:
            for row in response.css(
                "table#tblTransactionSearchResult tr:nth-child(n+3)"
            ):  # We start at the third row to avoid the header
                dico_data = {
                    "Transaction_ID": row.css("td:nth-child(1) span::text")
                    .get(default="")
                    .strip(),
                    "Transaction_Type": row.css("td:nth-child(2) span::text")
                    .get(default="")
                    .strip(),
                    "Transaction_Date": row.css("td:nth-child(3) span::text")
                    .get(default="")
                    .strip(),
                    "Transaction_Status": row.css("td:nth-child(4) span::text")
                    .get(default="")
                    .strip(),
                    "Transferring_Registry": row.css("td:nth-child(5) span::text")
                    .get(default="")
                    .strip(),
                    "Transferring_Account_Type": row.css("td:nth-child(6) span::text")
                    .get(default="")
                    .strip(),
                    "Transferring_Account_Name": row.css("td:nth-child(7) span::text")
                    .get(default="")
                    .strip(),
                    "Transferring_Account_Identifier": row.css(
                        "td:nth-child(8) span::text"
                    )
                    .get(default="")
                    .strip(),
                    "Transferring_Account_Holder": row.css("td:nth-child(9) span::text")
                    .get(default="")
                    .strip(),
                    "Acquiring_Registry": row.css("td:nth-child(10) span::text")
                    .get(default="")
                    .strip(),
                    "Acquiring_Account_Type": row.css("td:nth-child(11) span::text")
                    .get(default="")
                    .strip(),
                    "Acquiring_Account_Name": row.css("td:nth-child(12) span::text")
                    .get(default="")
                    .strip(),
                    "Acquiring_Account_Identifier": row.css(
                        "td:nth-child(13) span::text"
                    )
                    .get(default="")
                    .strip(),
                    "Acquiring_Account_Holder": row.css("td:nth-child(14) span::text")
                    .get(default="")
                    .strip(),
                    "Nb_of_Units": row.css("td:nth-child(15) span::text")
                    .get(default="")
                    .strip(),
                }
                yield dico_data
