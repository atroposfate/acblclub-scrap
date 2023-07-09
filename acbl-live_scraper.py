import scrapy
from scrapy.crawler import CrawlerProcess
import re
import json

class ACBL_spider(scrapy.Spider):
    name = 'acbl_club_spider'
    start_urls = ['https://my.acbl.org/club-results/details/817779']

    def start_requests(self):
        headers = {'User-Agent': 'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16'}
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.errback_http, headers=headers)


    def parse(self, response):
        # Check if the response has a successful status code
        if response.status == 200:

            # Extract the JavaScript code containing the 'data' variable
            script_code_match = re.search(r'var data = (.*?);', response.body.decode('utf-8'))
            if script_code_match:
                script_code = script_code_match.group(1)

                # Load the data as a JSON object
                data = json.loads(script_code)
                with open('extracted_data.json', 'w') as f:
                    json.dump(data, f)


            else:
                self.logger.error("Unable to find 'data' variable in the response")
        else:
            self.logger.error(f"Received a non-200 status code: {response.status}")

    def errback_http(self, failure):
        # Handle HTTP errors and exceptions
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error(f"HTTP Error {response.status} occurred for URL: {response.url}")
        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error(f"DNS Lookup Error occurred for URL: {request.url}")
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error(f"Timeout Error occurred for URL: {request.url}")
        else:
            self.logger.error(f"Error occurred: {failure.getErrorMessage()}")



if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ACBL_spider)
    process.start()