import json
from pathlib import Path

def extract_information(filename: Path) -> None:
  """
  Extracts information from an html file with JSON-LD content and prints it in a readable format.

  Args:
    filename: The name of html file.
  """
  head = 'ld+json'
  segment = '{"@context":"https://schema.org"'
  tail = '</script>'
  start = 0
  with open(filename, 'r') as f:
      raw = f.read()
      while start >= 0:
          index = raw.find(head, start, -1)
          if index == -1:
            start = -1
            break
          index = raw.find(segment, index, -1)
          if index == -1:
            start = -2
            break
          else:
            start = index
          index = raw.find(tail, index, -1)
          if index == -1:
            start = -3
            break
          else:
            end = index
          data = json.loads(raw[start:end])
          # Print general information about the file
          print(f"@context: {data['@context']}")
          print(f"@type: {data['@type']}")

          # Iterate over each item in the list
          for element in data['itemListElement']:
            pdb.set_trace()
            print(f"\n--- Item {element['position']} ---")
            item = element['item']
            if type(item) == dict:
                print(f"  @id: {item['@id']}")
                print(f"  name: {item['name']}")
                print(f"  brand: {item['brand']['name']}")  # Accessing the brand name
                print(f"  image: {item['image']}")

                # Check if description exists
                if 'description' in item:
                  print(f"  description: {item['description']}")

                print(f"  mpn: {item['mpn']}")
                print(f"  sku: {item['sku']}")

                # Access offer information
                offer = item['offers']
                print(f"  lowPrice: {offer['lowPrice']}")
                print(f"  highPrice: {offer['highPrice']}")
                print(f"  priceCurrency: {offer['priceCurrency']}")

                print(f"  price: {offer.get('price', '-')}")
                print(f"  availability: {offer.get('availability', '-')}")
                print(f"  itemCondition: {offer.get('itemCondition', '-')}")
                print(f"  priceValidUntil: {offer.get('priceValidUntil', '-')}")

                print(f"  offerCount: {offer.get('offerCount','-')}")
                print(f"  gtin: {item.get('gtin', '-')}")
            elif type(item) == str:
              print(item)
