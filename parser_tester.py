import lxml.html, lxml.etree
import csv

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

with open('html_pages/2.html') as file:
    html_data = file.read()
    print(html_data)
    parse(html_data)