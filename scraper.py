import re
from urllib.parse import urlparse
import utils.response
from bs4 import BeautifulSoup
from collections import defaultdict

class Scraper:
    visited_pages: set[str] = set()    # set of all unique urls visited
    longest_page: tuple[str, int] = ("", -1)    # (url, num words)
    word_count: defaultdict[str, int] = defaultdict(int)    # dict[word] = count
    ics_subdomains: defaultdict[str, int] = defaultdict(int)    # dict[ics subdomain] = num unique pages
    ENGLISH_STOPWORDS: set[str] = {'a', 'about', 'above', 'after', 'again', 'against',  'all', 'am', 'an', 
        'and', 'any', 'are', "aren't", 'as',  'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 
        'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', 
        "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', 
        "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 
        'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 
        'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 
        'only', 'or', 'other', 'ought', 'our', 'ours\tourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", 
        "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 
        'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 
        'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", 
        "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 
        'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 
        'yours', 'yourself', 'yourselves'}

    def __init__(self) -> None:
        pass
        
    def scraper(self, url: str, resp: utils.response.Response) -> list:
        # This function needs to return a list of urls that are scraped from the response.
        # An empty list for responses that are empty. 
        # These urls will be added to the Frontier and retrieved from the cache.
        # These urls have to be filtered so that urls that do not have to be downloaded are not added to the frontier.
        # The first step of filtering the urls can be by using the is_valid function provided in the same scraper.py file. 
        # Additional rules should be added to the is_valid function to filter the urls.
        links = self.extract_next_links(url, resp)
        return [link for link in links if is_valid(link)]

    def extract_next_links(self, url: str, resp: utils.response.Response):
        # Implementation required.
        # url: the URL that was used to get the page
        # resp.url: the actual url of the page
        # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
        # resp.error: when status is not 200, you can check the error here, if needed.
        # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
        #         resp.raw_response.url: the url, again
        #         resp.raw_response.content: the content of the page!
        # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
        #
        # if website.permitsCrawl(url):
        #   text = retrieveURL(url)
        #   storeDocument(url, text)
        #   for each url in parse(text):
        #       frontier.addURL(url)
        parsed_url = urlparse(url, allow_fragments=False)
        if resp.status != 200 or resp.raw_response is None or parsed_url in Scraper.visited_pages:
            return list()
        Scraper.visited_pages.add(parsed_url)

        next_links = []

        # referenced https://www.geeksforgeeks.org/beautifulsoup-scraping-link-from-html/ for bs4 usage
        # referenced https://medium.com/quantrium-tech/extracting-words-from-a-string-in-python-using-regex-dac4b385c1b8 for extracting words using re
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
        page_words = re.findall("[a-z0-9]+", soup.get_text().lower())    # define a word = sequence of alphanumeric char (lowercase a-z AND digits 0-9)
        self.update_longest_page_and_word_count(page_words, resp.url)
        for link in soup.find_all("a"):
            new_url = link.get("href")
            if new_url and is_valid(new_url):
                next_links.append(new_url)

        # check for redirect, url = original url | resp.raw_response.url = redirected url
        if url != resp.raw_response.url:
            next_links.append(resp.raw_response.url)

        return next_links
    
    def update_longest_page_and_word_count(self, words: list[str], url: str):
        # first update longest page (do not filter stopwords)
        if len(words) > Scraper.longest_page[1]:
            Scraper.longest_page = (url, len(words))
        # next, update word count (filter out stopwords)
        for word in words:
            if word not in Scraper.ENGLISH_STOPWORDS:
                Scraper.word_count[word] += 1


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        # check if url is in the domain (https://regexr.com/ helped me figure out the right expression)
        if not re.match(r".*\.(ics|cs|informatics|stat)\.uci\.edu/.*", parsed.geturl()):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz"
            + r"|pdf|ppt|pptx|doc|docx|css|js)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

# TODO:
# Track visited pages
# Crawl pages with high textual content
# Detect and avoid infinite traps
# Detect and avoid sets of similar pages with no information
# Detect redirects and if the page redirects, index the redirected content (done)
# Detect and avoid dead URLs that return a 200 status but no data (done)
# Detect and avoid crawling large files, especially if they have low information value

# REPORT:
#   1. How many unique pages (discard fragment) (disregard textual similarity)
#   2. What is longest page (disregard html markup)
#   3. What are the 50 most common words (ignore english stop words)
#   4. How many subdomains in ics.uci.edu domain (list alphabetically and by num. unique pages in sub-dom)

# EC: Implement checks and usage of the robots and sitemap files
# EC: Implement exact and near webpage similarity detection using lecture method