import re
from urllib.parse import urlparse, urljoin, urldefrag, urlunparse
from urllib.robotparser import RobotFileParser
import utils.response
from bs4 import BeautifulSoup
from collections import defaultdict

class Scraper:
    visited_pages: set[str] = set()    # set of all unique urls visited
    pages_in_front: set[str] = set()    # pages that are in frontier, in a set for O(1) lookup
    longest_page: tuple[str, int] = ("", -1)    # (url, num words)
    word_count: defaultdict[str, int] = defaultdict(int)    # dict[word] = count
    ics_subdomains: defaultdict[str, int] = defaultdict(int)    # dict[ics subdomain] = num unique pages
    all_fingerprints: list[set[int]] = []    # all fingerprints
    robot_allowed: dict[str, bool] = defaultdict(bool)
    ENGLISH_STOPWORDS: set[str] = {'a', 'about', 'above', 'after', 'again', 'against',  'all', 'am', 'an', 
        'and', 'any', 'are', "aren't", 'as',  'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 
        'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', 
        "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', 
        "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 
        'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 
        'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 
        'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", 
        "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 
        'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 
        'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", 
        "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 
        'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 
        'yours', 'yourself', 'yourselves'}
    PAGE_MIN_SIZE: int = 250    # min number of words a page should have
    PAGE_MAX_SIZE: int = 10_000    # max number of words a page should have
    PAGE_SIMILARITY_THRESHOLD = 0.9    # threshold for two pages to be 'similar'
    TRAP_FINGERPRINT_CHECK = 25    # how many recent previous pages to compare against for traps/cycles

    def __init__(self) -> None:
        pass
        
    def scraper(self, url: str, resp: utils.response.Response) -> list[str]:
        # This function needs to return a list of urls that are scraped from the response.
        # An empty list for responses that are empty. 
        # These urls will be added to the Frontier and retrieved from the cache.
        # These urls have to be filtered so that urls that do not have to be downloaded are not added to the frontier.
        # The first step of filtering the urls can be by using the is_valid function provided in the same scraper.py file. 
        # Additional rules should be added to the is_valid function to filter the urls.
        links = self.extract_next_links(url, resp)
        return [link for link in links if is_valid(link)]

    def extract_next_links(self, url: str, resp: utils.response.Response) -> list[str]:
        # Implementation required.
        # url: the URL that was used to get the page
        # resp.url: the actual url of the page
        # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
        # resp.error: when status is not 200, you can check the error here, if needed.
        # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
        #         resp.raw_response.url: the url, again
        #         resp.raw_response.content: the content of the page!
        # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

        # parse the url and do basic checks to confirm validity of url
        parsed_url = urlparse(resp.url.lower(), allow_fragments=False)
        defrag_url = urldefrag(resp.url.lower()).url
        # perform robots.txt check first - referenced https://docs.python.org/3/library/urllib.robotparser.html for help
        try:
            # first check if we already checked robots.txt for this domain
            robot_url = urlunparse((parsed_url.scheme, parsed_url.netloc, "/robots.txt", "", "", ""))
            if robot_url in Scraper.robot_allowed and not Scraper.robot_allowed[robot_url]:
                return list()
            # read robots.txt and save whether we are allowed to
            rfp = RobotFileParser()
            rfp.set_url(robot_url)
            rfp.read()
            robot_can_read = rfp.can_fetch("IR US24 70346322", defrag_url)
            Scraper.robot_allowed[robot_url] = robot_can_read
            if not robot_can_read:
                return list()
        except:
            pass
        if resp.status != 200 or not resp or not resp.raw_response \
           or defrag_url in Scraper.visited_pages or not is_valid(defrag_url):
            return list()
        
        # after basic checks, mark the link as 'visited' and update ics subdomain tracker
        Scraper.visited_pages.add(defrag_url)
        Scraper.pages_in_front.discard(defrag_url)
        if re.match(r".*\.ics\.uci\.edu", parsed_url.netloc):
            Scraper.ics_subdomains[urlunparse((parsed_url.scheme, parsed_url.netloc, "", "", "", ""))] += 1

        # referenced https://www.geeksforgeeks.org/beautifulsoup-scraping-link-from-html/ for bs4 usage
        # referenced https://medium.com/quantrium-tech/extracting-words-from-a-string-in-python-using-regex-dac4b385c1b8 for extracting words using re
        
        # parse the page for text and perfrom further validity tests on the page before extracting urls
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
        page_words = re.findall("[a-z0-9]+", soup.get_text().lower())    # define a word = sequence of alphanumeric char (lowercase a-z AND digits 0-9)
        if not self.page_is_valid_size(page_words):
            return list()
        
        # check for trap and similarity using page fingerprint method from lecture
        page_fingerprint = self.create_fingerprint(page_words)
        if self.check_for_recent_trap(page_fingerprint):
            return list()
        Scraper.all_fingerprints.append(page_fingerprint)

        # update counting stats 
        self.update_longest_page_and_word_count(page_words, resp.url.lower())

        # extract urls from the page to add them to the frontier
        next_links = []
        for link in soup.find_all("a"):
            new_url = urljoin(resp.url.lower(), link.get("href"))    # turn relative url to absolute if needed;
            new_url = urldefrag(new_url).url
            if new_url and is_valid(new_url) and new_url not in Scraper.visited_pages and new_url not in Scraper.pages_in_front:
                next_links.append(new_url)
                Scraper.pages_in_front.add(new_url)

        # check for redirect, url = original url | resp.url = redirected url
        if url.lower() != resp.url.lower():
            Scraper.visited_pages.add(urldefrag(url.lower()).url)

        return next_links
    
    def update_longest_page_and_word_count(self, words: list[str], url: str) -> None:
        # first update longest page (do not filter stopwords)
        if len(words) > Scraper.longest_page[1]:
            Scraper.longest_page = (url, len(words))
        # next, update word count (filter out stopwords)
        for word in words:
            if word not in Scraper.ENGLISH_STOPWORDS:
                Scraper.word_count[word] += 1

    def page_is_valid_size(self, words: list[str]) -> bool:
        # return whether the number of words in the page is in a predefined interval
        return Scraper.PAGE_MIN_SIZE <= len(words) and \
               len(words) <= Scraper.PAGE_MAX_SIZE
    
    @staticmethod
    def ouput_crawl_statistics(filename: str = "crawl_summary.txt") -> None:
        with open(filename, "w") as file:
            file.write("CS 121/INF 141 - Assignment 2: Web Crawler - Crawl Summary\n")
            file.write("IR US24 70346322\n\n")
            file.write(f"Total unique pages found = {len(Scraper.visited_pages)}\n\n")
            file.write(f"Longest page = {Scraper.longest_page[0]} with {Scraper.longest_page[1]} words\n\n")
            file.write(f"Top 50 most common words (excluding stop words):\n")
            for key, value in (sorted(Scraper.word_count.items(), key=lambda item: item[1]))[:50]:
                file.write(f"\t{key}\n")
            file.write("\n")
            file.write(f"ics.uci.edu Subdomains:\n")
            for key, value in sorted(Scraper.ics_subdomains.items(), key=lambda item: (item[0], item[1])):
                file.write(f"\t{key}, {value}\n")

    def create_fingerprint(self, words: list[str]) -> set[int]:
        # create 3 grams
        three_grams = (words[i:i+3] for i in range(len(words)-2))
        # calculate hash values of 3 grams
        three_gram_hashes = (hash(tuple(gram)) for gram in three_grams)
        # select hash values using mod 4
        return set(gram_hash for gram_hash in three_gram_hashes if gram_hash % 4 == 0)

    def fingerprints_are_similar(self, fingerprint_1: set[int], fingerprint_2: set[int]) -> bool:
        # similarity if intersection(fingerprints) / union(fingerprints) >= threshold
        intersection = fingerprint_1 & fingerprint_2
        union = fingerprint_1 | fingerprint_2
        if union:
            similarity = len(intersection) / len(union) 
        else:
            similarity = int(len(intersection) == len(union))
        return similarity >= Scraper.PAGE_SIMILARITY_THRESHOLD
    
    def check_for_recent_trap(self, fingerprint: set[int]) -> bool:
        # only start checking for traps once enough fingerprints have been stored
        if len(Scraper.all_fingerprints) <= Scraper.TRAP_FINGERPRINT_CHECK:
            return False
        # compare fingerprint arg to recent fingerprints for their similarity
        start_index_incl = len(Scraper.all_fingerprints)-2
        end_index_excl = len(Scraper.all_fingerprints)-2-Scraper.TRAP_FINGERPRINT_CHECK
        for i in range(start_index_incl, end_index_excl, -1):
            if self.fingerprints_are_similar(fingerprint, Scraper.all_fingerprints[i]):
                return True
        return False


def is_valid(url) -> bool:
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        # check if url is in the domain (https://regexr.com/ helped me figure out the right expression)
        if not re.match(r".*\.(ics|cs|informatics|stat)\.uci\.edu$", parsed.netloc):
            return False
        # if re.match(r"\/doku\.php\/.*", parsed.path):
        #     return False
        # if re.match(r"\/~eppstein\/pix\/.*", parsed.path):
        #     return False
        # if re.match(r".*grape\.ics\.uci\.edu.*", parsed.netloc):
        #     return False
        if re.match(r".*\/pdf.*", parsed.path):
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
            + r"|java|war|jar"
            + r"|pdf|ppt|pptx|doc|docx|css|js)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
