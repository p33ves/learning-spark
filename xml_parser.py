import xml.etree.ElementTree as ET

xml_file = "data\philosophy\meta\Badges.xml"
tree = ET.parse(xml_file)

root = tree.getroot()
for child in root:
    print(child.attrib)
