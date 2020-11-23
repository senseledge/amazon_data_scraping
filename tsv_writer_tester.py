import csv

with open('test.tsv', 'a') as tsvfile:
    fieldnames = ['description', 'price', 'prime', 'link', 'stars']
    writer = csv.DictWriter(tsvfile, fieldnames=fieldnames, delimiter="\t")
    #writer.writeheader()
    writer.writerow({'description': 'PC1', 'price': 99.99, 'prime': True, 'link': "www.amazon.it", 'stars': 4.7})
    writer.writerow({'description': 'PC2', 'price': 150.00, 'prime': False, 'link': "www.amazon.it", 'stars': 2.5})
    writer.writerow({'description': 'PC3', 'price': 11.00, 'prime': False, 'link': "www.amazon.it", 'stars': 4.5})