import asyncio, aiohttp
import time, random
import lxml.html, lxml.etree
import csv

# Base URL
url = "https://www.amazon.it/s?k=computer&page={}"

# Fake User Agent to prevent Amazon blocking
user_agent_list = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
]

# Generate the URLs for all the search results pages
def generate_urls(model_url):
    urls = []
    for n in range(3, 30):
        n_url = model_url.format(n)
        urls.append(n_url)

    return urls

def tsv_writer(item_data):
    with open('items_data.tsv', 'a') as tsvfile:
        fieldnames = ['description', 'price', 'prime', 'link', 'stars']
        writer = csv.DictWriter(tsvfile, fieldnames=fieldnames, delimiter="\t")
        # writer.writeheader()
        writer.writerow(item_data)

# HTML parser to extract the interesting data
def parse(responce_source):

    XPATH_ITEM = "//div[@data-component-type='s-search-result']"
    XPATH_DESCRIPTION = ".//h2/a/span/text()"
    XPATH_LINK = ".//h2/a/@href"
    XPATH_PRIME = ".//i[contains(@class, 'a-icon-prime')]/@aria-label"
    XPATH_STARS = ".//i[contains(@class, 'a-icon-star-small')]/span/text()"
    XPATH_PRICE = ".//span[@class='a-price-whole']/text()"

    html_page = lxml.etree.HTML(responce_source)
    items = html_page.xpath(XPATH_ITEM)

    for item in items:
        description = item.xpath(XPATH_DESCRIPTION)[0]
        stars = item.xpath(XPATH_STARS)[0].replace(' su 5 stelle', '') if item.xpath(XPATH_STARS) != [] else 'Null'
        prime = True if item.xpath(XPATH_PRIME) != [] else False
        link = "https://www.amazon.it" + item.xpath(XPATH_LINK)[0]
        price = item.xpath(XPATH_PRICE)[0] if item.xpath(XPATH_PRICE) != [] else 'Null'

        item_row = {'description': description, 'price': price, 'prime': prime, 'link': link, 'stars': stars}

        tsv_writer(item_row)

# Request the web site page
async def download_site(session, url):
    time.sleep(random.random()*8)
    print(url)
    async with session.get(url) as response:
        print(response.content_length)
        content = await response.read()
        parse(content)

# Request all the web site pages
async def download_all_sites(sites):
    user_agent = random.choice(user_agent_list)
    # Set the headers
    headers = {'User-Agent': user_agent}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []
        for url in sites:
            task = asyncio.ensure_future(download_site(session, url))
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

# Scraping main
if __name__ == "__main__":
    sites = generate_urls(url)
    start_time = time.time()
    asyncio.get_event_loop().run_until_complete(download_all_sites(sites))
    duration = time.time() - start_time
    print(f"Downloaded {len(sites)} sites in {duration} seconds")

