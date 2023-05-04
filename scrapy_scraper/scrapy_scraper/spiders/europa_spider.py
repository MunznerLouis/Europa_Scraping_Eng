import asyncio
import logging
import scrapy  # pip install scrapy  --> scrapy ver. > 2.4 to use asyncio

# when the command to start the spider is executed, it's the start_requests() method that is called first
class europa_spider(scrapy.Spider):
    """
        A spider class for collecting compliances from the EU ETS registry.

        This spider class uses the Scrapy framework to collect data from the Operator Holding accounts in the EU ETS
        (European Union Emission Trading System) registry. It uses the asyncio and scrapy libraries to perform asynchronous
        requests and parse web pages. The class is configured to handle requests, responses, and perform the necessary actions
        to collect transaction data and store it in a CSV file.

    Attributes:

        name (str): The name of the spider.
        start_urls (str): The starting URL for collecting data from Operator Holding accounts.
        custom_settings (dict): A dictionary of custom parameters for spider configuration.

    Methods:
        start_requests(): A method for starting spider requests.
        parse_pages(response): A method for parsing data pages from Operator Holding accounts.
        parse(response): A method for parsing Operator Holding account data on web pages.
        parse_compliances(response): A method for parsing Operator Holding account data from the 2nd page.
    """
    name = "europa_spider"
    start_urls = "https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=accountTypeCode+ASC&backList=%3CBack&resultList.currentPageNumber=2"
    custom_settings = {
        "LOG_LEVEL": "INFO",
    }

    # override of the start_requests() method to call the parse_pages() method instead of the parse() method
    def start_requests(self):
        """
            Override the start_requests function so that the first function called is 'parse_pages' instead of 'parse'.

        Yields:

            scrapy.Request: The request to be processed by the 'parse_pages' callback function.
        """
        yield scrapy.Request(
            "https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=accountTypeCode+ASC&backList=%3CBack&resultList.currentPageNumber=2",
            callback=self.parse_pages,
        )

    # function to know the number of pages to parse and to set up the columns of the .csv
    def parse_pages(self, response):
        """Method to parse the number of pages to parse and to set up the columns of the .csv. 

        Args:
            response (scrapy.http.Response): the http response to be parsed.

        Yields:
            scrapy.http.Request: A http request to be processed by the 'parse' callback function.
        """
        pages = response.css("td.bgpagecontent input:nth-child(5)::attr(value)").get()

        # allow to set up the columns of the .csv correctly, since the header is set up statically at the start
        yield response.follow(
            self.start_urls, callback=self.parse, meta={"page": 1}, priority=1
        )

        for page in range(3, int(pages) + 2):
            url = (
                f"https://ec.europa.eu/clima/ets/oha.do?form=oha&languageCode=fr&accountHolder=&installationIdentifier=&installationName=&permitIdentifier=&mainActivityType=-1&searchType=oha&currentSortSettings=accountTypeCode+ASC&backList=%3CBack&resultList.currentPageNumber="
                + str(page)
            )
            yield response.follow(url, callback=self.parse, meta={"page": page - 1})

    # extract the data from the web page
    def parse(self, response):
        """Parse function to extract data from the web page.

        Args:
            response (scrapy.http.Response): the object containing the response to be parsed.

        Yields:
            dict:a dictionnary containing the data extracted from the web page.
        """
        page = response.meta["page"]
        print(
            "page",
            page,
            "sur",
            response.css("td.bgpagecontent input:nth-child(5)::attr(value)").get(),
        )

        # if there was an error in the response, we try again
        if (
            response.css("td.bgpagecontent input:nth-child(5)::attr(value)").get()
            is None
        ):
            logging.warning(f"Page content is None for {response.url}, retrying...")
            yield response.follow(
                response.url, callback=self.parse, meta={"page": page + 1}
            )
        else:
            for row in response.css(
                "table#tblAccountSearchResult tr:nth-child(n+3)"
            ):  #we start at tr:nth-child(n+3) since the infos we want start at the 3rd row
                dico_table_data = {
                    "National_Administrator": row.css("td:nth-child(1) span::text")
                    .get(default=None)
                    .strip(),
                    "Account_Type": row.css("td:nth-child(2) span::text")
                    .get(default=None)
                    .strip(),
                    "Account_Holder_Name": row.css("td:nth-child(3) span::text")
                    .get(default=None)
                    .strip(),
                    "Installation/Aircraft_ID": row.css("td:nth-child(4) span::text")
                    .get(default=None)
                    .strip(),
                    "Installation_Name/Aircraft_Operator_Code": row.css(
                        "td:nth-child(5) span::text"
                    )
                    .get(default=None)
                    .strip(),
                    "Company_Regustration_No": row.css("td:nth-child(6) span::text")
                    .get(default=None)
                    .strip(),
                    "Permit/Plan_ID": row.css("td:nth-child(7) span::text")
                    .get(default=None)
                    .strip(),
                    "Permit/Plan_Date": row.css("td:nth-child(8) span::text")
                    .get(default=None)
                    .strip(),
                    "Main_Activity_Type": row.css("td:nth-child(9) span::text")
                    .get(default=None)
                    .strip(),
                    "Latest_Compliance_Code": row.css("td:nth-child(10) span::text")
                    .get(default=None)
                    .strip(),
                }
                url = row.css("td:nth-child(11) td:nth-child(2) a::attr(href)").get()
                if url:
                    # allows us to go trough the compliance page and extract the data
                    yield response.follow(
                        url,
                        self.parse_compliances,
                        meta={"dico_table_data": dico_table_data},
                    )

    def parse_compliances(self, response):
        """Method to extract data from the compliance page. 

        Args:
            response (scrapy.http.Response): response of the http request.

        Yields:
            dict: dictionnary containing the data extracted from the website.
        """
        dico_table_data = response.meta["dico_table_data"]

        # General Information
        dico_table_data["Account_Status"] = (
            response.css(
                "table#tblAccountGeneralInfo tr:nth-child(3) td:nth-child(6) span::text"
            )
            .get(default="")
            .strip()
        )
        # the strip part is mandatory, else we pick up data under this format : '&nbsp;Operator Holding Account&nbsp;'
        # Details on Contact Information
        dico_table_data["Type"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(1) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Legal_Entity_Identifier"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(3) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Main_Adress_Line"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(4) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Secondary_Adress_Line"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(5) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Postal_Code"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(6) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["City"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(7) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Country"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(8) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Telephone_1"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(9) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Telephone_2"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(10) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["E-Mail_Adress"] = (
            response.css(
                "table#tblAccountContactInfo tr:nth-child(3) td:nth-child(11) span::text"
            )
            .get(default="")
            .strip()
        )

        # other General Information
        dico_table_data["Monitoring_plan—year_of_expiry"] = (
            response.css(
                "table#tblChildDetails table:nth-child(1) tr:nth-child(3) td:nth-child(5) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Name_of_Subsidiary_undertaking"] = (
            response.css(
                "table#tblChildDetails table:nth-child(1) tr:nth-child(3) td:nth-child(6) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Name_of_Parent_undertaking"] = (
            response.css(
                "table#tblChildDetails table:nth-child(1) tr:nth-child(3) td:nth-child(7) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["E-PRTR_identification"] = (
            response.css(
                "table#tblChildDetails table:nth-child(1) tr:nth-child(3) td:nth-child(8) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Call_Sign_(ICAO_designator)"] = (
            response.css(
                "table#tblChildDetails table:nth-child(1) tr:nth-child(3) td:nth-child(9) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["First_Year_of_Emissions"] = (
            response.css(
                "table#tblChildDetails table:nth-child(1) tr:nth-child(3) td:nth-child(10) span::text"
            )
            .get(default="")
            .strip()
        )
        dico_table_data["Last_Year_of_Emissions"] = (
            response.css(
                "table#tblChildDetails table:nth-child(1) tr:nth-child(3) td:nth-child(11) span::text"
            )
            .get(default="")
            .strip()
        )

        #get the data from the table with only one table
        if response.css("[id=tblChildDetails] div:nth-child(2)") == []:
            for row in response.css(
                "[id=tblChildDetails] div table tr:nth-child(n+3)"
            ):  # (-n+6) since the last 7 rows are not needed and prouced errors
                key_year = row.css("td:nth-child(2) span::text").get(default="").strip()
                if len(key_year) == 4:
                    for cell in row.css("tr"):
                        dico_table_data[
                            "EU_Compliance_" + key_year + "_Allowances_in_Allocation"
                        ] = (
                            cell.css("td:nth-child(3) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_" + key_year + "_Verified_Emissions"
                        ] = (
                            cell.css("td:nth-child(4) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_" + key_year + "_Units_Surrendered"
                        ] = (
                            cell.css("td:nth-child(5) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_"
                            + key_year
                            + "_Cumulative_Surrendered_Units"
                        ] = (
                            cell.css("td:nth-child(6) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_"
                            + key_year
                            + "_Cumulative_Verified_Emissions"
                        ] = (
                            cell.css("td:nth-child(7) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_" + key_year + "_Compliance_Code"
                        ] = (
                            cell.css("td:nth-child(8) span::text")
                            .get(default="")
                            .strip()
                        )

        # get information related to EU copmliance and CH compliance when there are two tables
        else:

            for row in response.css(
                "[id=tblChildDetails] div:nth-child(1) table tr:nth-child(n+3)"
            ):  # (-n+6) since the last 7 rows are not needed and produces errors
                key_year = row.css("td:nth-child(2) span::text").get(default="").strip()
                if len(key_year) == 4:
                    for cell in row.css("tr"):
                        dico_table_data[
                            "EU_Compliance_" + key_year + "_Allowances_in_Allocation"
                        ] = (
                            cell.css("td:nth-child(3) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_" + key_year + "_Verified_Emissions"
                        ] = (
                            cell.css("td:nth-child(4) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_" + key_year + "_Units_Surrendered"
                        ] = (
                            cell.css("td:nth-child(5) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_"
                            + key_year
                            + "_Cumulative_Surrendered_Units"
                        ] = (
                            cell.css("td:nth-child(6) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_"
                            + key_year
                            + "_Cumulative_Verified_Emissions"
                        ] = (
                            cell.css("td:nth-child(7) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "EU_Compliance_" + key_year + "_Compliance_Code"
                        ] = (
                            cell.css("td:nth-child(8) span::text")
                            .get(default="")
                            .strip()
                        )

            # CH Compliance information - only AIRCRAFT OPERATOR ACCOUNT
            for row in response.css(
                "[id=tblChildDetails] div:nth-child(2) table tr:nth-child(n+5):not(:nth-last-child(-n+4))"
            ):  
                key_year = row.css("td:nth-child(2) span::text").get(default="").strip()
                if len(key_year) == 4:
                    for cell in row.css("tr"):

                        dico_table_data[
                            "CH_Compliance_" + key_year + "_Allowances_in_Allocation"
                        ] = (
                            cell.css("td:nth-child(3) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "CH_Compliance_" + key_year + "_Verified_Emissions"
                        ] = (
                            cell.css("td:nth-child(4) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "CH_Compliance_" + key_year + "_Units_Surrendered"
                        ] = (
                            cell.css("td:nth-child(5) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "CH_Compliance_"
                            + key_year
                            + "_Cumulative_Surrendered_Units"
                        ] = (
                            cell.css("td:nth-child(6) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "CH_Compliance_"
                            + key_year
                            + "_Cumulative_Verified_Emissions"
                        ] = (
                            cell.css("td:nth-child(7) span::text")
                            .get(default="")
                            .strip()
                        )
                        dico_table_data[
                            "CH_Compliance_" + key_year + "_Compliance_Code"
                        ] = (
                            cell.css("td:nth-child(8) span::text")
                            .get(default="")
                            .strip()
                        )

        # send the data to the pipeline to be stored in a csv file
        yield dico_table_data


