import xml.etree.ElementTree as ET

tree = ET.parse('netflix_data.xml')

root = tree.getroot()
for child in root:
    for attr in range(len(child)):
        print(f"{child[attr].tag}: {child[attr].text}")
