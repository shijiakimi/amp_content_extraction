import json
from kafka import KafkaConsumer
import logging
import os
from lxml.html import fromstring
from logging.handlers import TimedRotatingFileHandler
import extract_amp_page
import tldextract
import csv
import arrow
from pymongo import MongoClient


logger = logging.getLogger(__name__)
common_xpath = '(//h1)[1]'

kafka_consumer = None
kafka_host = '172.31.18.250:9092,172.31.27.13:9092,172.31.19.188:9092'


def config_log():
    # logging.getLogger("kafka").setLevel(logging.ERROR)
    if not os.path.isdir('logs'):
        os.mkdir('logs')
    formatter = logging.Formatter(fmt='%(asctime)s, %(name)s %(levelname)s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = TimedRotatingFileHandler('logs/' + 'amp_process' + '.log', when='D')
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)


def init_client(host, port):
    return MongoClient(host, port)


def init_collection(client, db, collection):
    return client[db][collection]


def generate_now_timestamp():
    return arrow.now().timestamp


def get_kafka_consumer(topic, kafka_host, group):
    return KafkaConsumer(topic, bootstrap_servers=kafka_host.split(','), group_id=group,
                         max_partition_fetch_bytes=1024 * 1024 * 10)


def normalize(url):
    try:
        if len(url) > 500:
            return None

        if not url.startswith('http'):
            return None

        if 'wp-login.php' in url:
            return None
        if 'abcnews.go.com' in url:
            return url
        url = url.split('?')[0].split('#')[0]
        if 'popsugar.com' in url:
            url += '?stream_view=1'
        elif 'therichest.com' in url:
            url += '?view=all'
        elif 'howstuffworks.com' in url:
            url += '/printable'
        return url
    except:
        logger.error('normalize url error {}'.format(url), exc_info=True)
        return None


def extract_title(root, title_xpath):
    title = extract_amp_page.extract_title(root, title_xpath)
    return title


def extract_clean_content(root, content_xpath):
    html = extract_amp_page.extract_content_html_by_xpath(root, content_xpath)
    return html


def extract_img_urls(root, img_xpath):
    return extract_amp_page.extract_img(root, img_xpath)

def get_registered_domain(url):
    ext = tldextract.extract(url)
    registered_domain = ext.registered_domain
    return registered_domain


def get_domain_xpath_dict(csvfile):
    special_dict = {}
    with open(csvfile, 'r') as readcsvfile:
        reader = csv.DictReader(readcsvfile)
        for row in reader:
            registered_domain = row['registered_domain']
            title_xpath = row['xpath']
            special_dict[registered_domain] = title_xpath
    return special_dict


def generate_white_list_set(file):
    with open(file, 'r') as read_f:
        lines = read_f.readlines()
        check_set = set()
        for url in lines:
            ext = tldextract.extract(url)
            registered_domain = ext.registered_domain
            check_set.add(registered_domain)
    return check_set


def process_record(msg_str, title_special_dict, content_dict, img_dict, white_list_domain):
    data = {}
    try:
        record = json.loads(msg_str)
        amp_url = record['url']
        canonical_url = record['metadata']['url']
        canonical_url = normalize(canonical_url)
        registered_domain = get_registered_domain(amp_url)
        if registered_domain not in white_list_domain:
            return {}

        title_xpath = title_special_dict.get(registered_domain, common_xpath)
        if not title_xpath:
            logger.info('need to add title xpath for: {}'.format(amp_url))
        content_xpath = content_dict.get(registered_domain)
        if not content_xpath:
            logger.info('need to add content xpath for: {}'.format(amp_url))
        img_xpath = img_dict.get(registered_domain)
        if not img_xpath:
            logger.info('need to add img xpath for: {}'.format(amp_url))
        content = record['content']
        if not content:
            return {}
        root = fromstring(content)
        amp_title = extract_title(root, title_xpath)
        amp_clean_content = extract_clean_content(root, content_xpath)
        amp_img_urls = extract_img_urls(root, img_xpath)
        data['amp_img_xpath'] = img_xpath
        data['amp_title_xpath'] = title_xpath
        data['amp_content_xpath'] = content_xpath
        data['amp_img_urls'] = amp_img_urls
        data['amp_img_num'] = len(amp_img_urls)
        if not amp_img_urls:
            logger.info('no img found, need to check img xpath for: {}'.format(amp_url))
        data['amp_url'] = amp_url
        data['url'] = canonical_url
        data['amp_extract_title'] = amp_title
        if not amp_title:
            logger.info('no title found, need to check title xpath for: {}'.format(amp_url))
        data['amp_clean_content'] = amp_clean_content
        if not amp_clean_content:
            logger.info('no clean content found, need to check content xpath for: {}'.format(amp_url))
        data['epoch'] = generate_now_timestamp()
        return data
    except:
        logger.error('fail to process record {}'.format(msg_str), exc_info=True)
        return data


def run(title_special_list_file, content_list_file, img_list_file, white_list_file):
    staging_host = '172.31.22.154'
    port = 27017
    amp_extraction_db = 'amp_extraction_correctness'
    amp_extraction_collec = 'amp_extraction_data'
    title_special_dict = get_domain_xpath_dict(title_special_list_file)
    content_dict = get_domain_xpath_dict(content_list_file)
    img_dict = get_domain_xpath_dict(img_list_file)
    white_list_domain = generate_white_list_set(white_list_file)
    amp_client = MongoClient(staging_host, port)
    amp_collection = amp_client[amp_extraction_db][amp_extraction_collec]

    kafka_consumer = get_kafka_consumer('amp_content', kafka_host, 'my_amp_extraction')
    for msg in kafka_consumer:
        data = process_record(msg.value, title_special_dict, content_dict, img_dict, white_list_domain)
        if not data:
            continue
        try:
            amp_collection.insert_one(data)
        except:
            logger.error('error inserting data', exc_info=True)


if __name__ == "__main__":
    # source_file = sys.argv[1]
    # url_tag = sys.argv[2]
    logging.basicConfig(format='%(asctime)s, %(name)s %(levelname)s %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO)
    config_log()
    title_special_list_file = "title_special_list_xpath.csv"
    white_list_file = 'whitelist.tsv'
    content_list_file = 'amp_content_xpath.csv'
    img_list_file = 'amp_img_xpath.csv'
    run(title_special_list_file, content_list_file, img_list_file, white_list_file)
