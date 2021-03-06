import requests
from lxml import html, etree
import sys
import logging
from urlparse import urljoin, urlparse

logger = logging.getLogger(__name__)


def get_ampurl_from_url(url):
    try:
        resp = requests.get(url, headers={
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'})
        root = html.fromstring(resp.content)
        ampurls = root.xpath('//link[@rel="amphtml"]/@href')
        # if len(ampurls) > 0:
        temp_ampurl = ampurls[0]
        ampurl = urljoin(urlparse(url).geturl(), temp_ampurl)
        # if ampurl.startswith('/'):
        #    parsed_url = urlparse(url)
        #    ampurl = parsed_url.scheme + '://' + parsed_url.hostname + ampurl
        # else:
        #    ampurl = None
        return ampurl
    except:
        logger.error('cannot request url to get amp url' + url, exc_info=True)
        return None


def get_content(url):
    try:
        resp = requests.get(url, headers={
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'})
        return resp.content.decode('utf-8')
    except:
        logger.error('cannot request url {}'.format(url))
        return None


def extract_content_html_by_xpath(root, content_xpath):
    if not content_xpath:
        return None
    try:
        nodes = root.xpath(content_xpath)
        if not nodes:
            return None
        lines = ''
        for element in nodes:
            if isinstance(element, basestring):
                continue
            line = etree.tostring(element)
            lines += line
        return lines
    except:
        logger.error('invalid content or xpath{}'.format(nodes), exc_info=True)
        return None



def extract_title(root, title_xpath):
    if not title_xpath:
        return None
    try:
        title = ""
        partial_titles = root.xpath(title_xpath)
        for partial_title in partial_titles:
            if isinstance(partial_title, basestring):
                continue
            partial_title = partial_title.text_content()
            if partial_title in title:
                continue
            title = title + partial_title
        return title
    except:
        logger.error('invalid nodes')
        return None


def extract_img(root, img_xpath):
    if not img_xpath:
        return []
    try:
        img_urls = root.xpath(img_xpath)
        return img_urls
    except:
        logger.error('fail to extract images by {}'.format(img_xpath), exc_info=True)
        return []


def write_to_file(to_be_written, file_path):
    with open(file_path, 'w') as f:
        f.write(to_be_written)


# extract html file from a url by xpath
def xpath_content_extraction(url, content_xpath, file_path):
    ampurl = get_ampurl_from_url(url)
    content = get_content(ampurl)
    root = html.fromstring(content)
    lines = extract_content_html_by_xpath(root, content_xpath)
    write_to_file(lines, file_path)


def xpath_title_extraction(url, title_xpath):
    ampurl = get_ampurl_from_url(url)
    content = get_content(ampurl)
    root = html.fromstring(content)
    title = extract_title(root, title_xpath)
    return title


# sys.argv[1] = url
# sys.argv[2] = content_xpat
# sys.argv[3] = path of the file to be written to
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s, %(name)s %(levelname)s %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)
    # xpath_extraction(sys.argv[1], sys.argv[2], sys.argv[3])
    title = xpath_title_extraction('http://ew.com/tv/2017/06/19/the-l-word-reunion-sex-scenes/',
                                   '//div[@class="content"]/h2/p/descendant-or-self::*')
    print title
