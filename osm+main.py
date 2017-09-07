
# coding: utf-8

# DATA WRANGLING: OPEN STREET MAP PROJECT
# 
# OSM File = Hyderabad
# 
# Hyderabad is the capital of the southern Indian state of Telangana and the de jure capital of Andhra Pradesh.  I've chosen Hyderabad  because, it's one most of the most commercial and developed cities in India.
# 
# Reference link for Hyderabad.osm : https://mapzen.com/data/metro-extracts/metro/hyderabad_india/

# Questions Explored :
# 
# •Firstly, I’ve identified some street types from the imported data by matching the last name of street name with regular expression function, I’ve found that many of the last names of the street name were entered wrongly or by using shortcuts (i.e. Street as St or St.etc..) and some street names ended with numerical numbers representing the serial number of respective street name and some ended with white spaces. 
# •Secondly, since Hyderabad is a district I want to differentiate Hyderabad city from Hyderabad district. This was done using the Postal Codes. Postal Codes ranging between 500010 and 500070 are considered to be in the city and the remaining are outside the city.
# 

# In[130]:

import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow

OSM_FILE = "hyderabad_india.osm"  # Replace this with your osm file
SAMPLE_FILE = "sample.osm"

k = 10 # Parameter: take every k-th top level element

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')


# STREET NAMES

# 1. Identifying the abbreivated street names

# In[1]:

import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

osm_file = r'D:\Udacity\hyderabad_india.osm'
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
street_types=defaultdict(set)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons" , "Nagar" , "Road" , "Tank" , "Lake" , "Valley" , "Enclave" ,
            "Apartment" , "Colony" , "Sector" , "Park" , "Nivas" , "Mall" , "Centre" , "Plaza"]

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")
street_types = defaultdict(set)
for event, elem in ET.iterparse(osm_file, events=("start",)):
    if elem.tag == "node" or elem.tag == "way":
        for tag in elem.iter("tag"):
            if is_street_name(tag): 
                audit_street_type(street_types, tag.attrib['v'])
    





# In[2]:

for i in street_types:
    print i


# Auditing Street names
# 
# To deal with correcting street names, I opted to use regular expressions, correcting them to their respective mappings in the update_street_name  function

# In[3]:

from collections import defaultdict
import re
import pprint
import xml.etree.cElementTree as ET

mapping = { "St": "Street",
            "udyog":"Udyog",
            "chaulk":"Chowk",
            "St.": "Street",
            "Ave":"Avenue",
            "chowk":"Chowk",
            "EFLU" :"",
            "Rd":"Road",
            "cross":"Cross",
            "Rd.":"Road",
            "nagar":"Nagar",
            "road":"Road",
            "raod":"Road",
            "apartment":"Apartment",
            "no.":"",
            "ROADS" : "Roads",
            "colony" : "Colony",
            "NH7" :"Highway"
           
        
            }
keys= mapping.keys() 

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
PROBLEMCHARS = re.compile(r'[=\+/&-<>;\'"\?%#$@\,\. \t\r\n]') 
LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+') 

def update_street_name(name, mapping):

        m = street_type_re.search(name)
        if m:
            street_type=m.group()
    
            if street_type in keys:
                value=mapping[street_type]
                y=name.find(street_type)
                z=name[:y]+value
                return z

            else:
                try: 
            
                    type(int(street_type))
                    position=name.find(street_type)
                    remove_numbers=name[:position]
                    return remove_numbers
          
                except ValueError:
                    x = name.replace(", "," ").replace(" ,"," ").replace(" No.","").replace(" no.","").replace(",","")
                    return x


# In[4]:

for event, elem in ET.iterparse(osm_file, events=("start",)):
    if elem.tag == "node" or elem.tag == "way": 
        for tag in elem.iter("tag"):
            if is_street_name(tag):
                print "Before:",tag.attrib['v']
                print "After:",update_street_name(tag.attrib['v'], mapping)


# POSTAL CODES

# In the first iteration of postal code Auditing ,I found and updated the postal codes which has white space characters present in postal codes and also the range of postal codes that lie in the city limits

# In[5]:

import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

pin_outside = []
pin_inside = []
i = 0
white_space=re.compile(r'\S+\s+\S+')
for event , elem in ET.iterparse(osm_file):
    if elem.tag == "node" or elem.tag == "way":
        for tag in elem.iter("tag"):
            if tag.attrib['k'] == "postal_code" or tag.attrib['k'] == "addr:postcode":
                if white_space.search(tag.attrib['v']):
                    i=i+1
                    continue
                elif int(tag.attrib['v'].strip())<500010 or int(tag.attrib['v'].strip())>500070:
                     pin_outside.append(tag.attrib['v'])
                        
                elif int(tag.attrib['v'].strip())>500010 or int(tag.attrib['v'].strip())<500070:
                     pin_inside.append(tag.attrib['v'])
                        
print "Number of postal codes wrongly entered :",i                    
print "Number of Postal codes which line outside the city : ",len(pin_outside)
print "Number of Postal codes which belong to city limits 500010-500070 :",len(pin_inside)

    


# In the second iteration,I found that some Postal codes were entered as string 

# In[6]:

white_space=re.compile(r'\S+\s+\S+')
COLON= re.compile(r'^([a-z]|_)+:')
def update_pincode(pincode):
    if white_space.search(pincode):
        x=pincode.replace(" ","") 
        return x 
    elif tag.attrib['v']=='Vikrampuri':
         return None
    elif COLON.search(pincode):
         return None
    elif int(pincode)<500010 or int(pincode)>500070:
         return None
    else:
        return pincode


# and In third iteration I found some postal codes wrongly entered as a string and colon “ : ” present in the string with help of regular expression(“^([a-z]|_)+:”) I’ve updated the both postal codes as None.

# In[7]:

for event, elem in ET.iterparse(osm_file):
    if elem.tag == "node" or elem.tag == "way":
        for tag in elem.iter("tag"):
            if tag.attrib['k'] == "postal_code" or tag.attrib['k'] == "addr:postcode":
                print "Before :",tag.attrib['v']
                print "After :",update_pincode(tag.attrib['v'])


# After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
# To do so we will parse the elements in the OSM XML file, transforming them from document format to
# tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
# imported to a SQL database as tables.
# 

# In[8]:

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET

import cerberus

import schema

OSM_PATH = "hyderabad_india.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements
   
    # YOUR CODE HERE
    if element.tag == 'node': 
        for k in element.attrib: 
            if k in NODE_FIELDS: 
                node_attribs[k]=element.attrib[k] 
        for y in element: 
            if y.tag=='tag': 
                node_tags = {}
                # tag k = 'addr:street' v = ' kormangala'
                # tag k = 'hourse ' v = 'asome'
                node_tags['id']=element.attrib['id'] 
                if LOWER_COLON.match(y.attrib['k']): 
                    
                    node_tags['key']=y.attrib['k'].split(":",1)[1] 
                    node_tags['type']=y.attrib['k'].split(":",1)[0] 
                else: 
                    node_tags['key'] = y.attrib['k']
                    node_tags['type'] = 'regular'
                    
                if y.attrib["k"] == 'addr:street':
                    if update_street_name(y.attrib["v"] , mapping):
                        node_tags["value"] = update_street_name(y.attrib["v"] , mapping)
                    else:
                        continue
                
                elif y.attrib["k"] == "addr:postcode":
                    if update_pincode(y.attrib["v"]):
                        node_tags["value"] = update_pincode(y.attrib["v"])
                    else:
                        continue
                else:
                    node_tags["value"] = y.attrib["v"]

                tags.append(node_tags)
                      
        return {'node': node_attribs, 'node_tags': tags} 
                
             

        
    elif element.tag == 'way': 
            for x in element.attrib: 
                if x in WAY_FIELDS: 
                    way_attribs[x]=element.attrib[x] 
            i=0 
            for q in element.iter("nd"): 
               
                way_nodes.append({'id':element.attrib['id'],'node_id':q.attrib['ref'],'position':i}) 
                i+=1 
            for y in element: 
                if y.tag=='tag': 
                    node_tags = {}
                    node_tags['id']=element.attrib['id']
                    if LOWER_COLON.match(y.attrib['k']): 
                         
                        node_tags['key']=y.attrib['k'].split(":",1)[1] 
                        node_tags['type']=y.attrib['k'].split(":",1)[0] 
                    else: 
                        node_tags['key'] = y.attrib['k']
                        node_tags['type'] = 'regular'
                    
                    if y.attrib["k"] == 'addr:street':
                        if update_street_name(y.attrib["v"] , mapping):
                            node_tags["value"] = update_street_name(y.attrib["v"] , mapping)
                        else:
                            continue
                    
                    elif y.attrib["k"] == "addr:postcode":
                        if update_pincode(y.attrib["v"]):
                            node_tags["value"] = update_pincode(y.attrib["v"])
                        else:
                            continue

                       
                    else:
                         node_tags["value"] = y.attrib["v"]
                        
                    tags.append(node_tags)
                   
            return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}       
    

# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file,          codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file,          codecs.open(WAYS_PATH, 'w') as ways_file,          codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file,          codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                    if element.tag == 'node':
                        nodes_writer.writerow(el['node'])
                        node_tags_writer.writerows(el['node_tags'])
                    elif element.tag == 'way':
                        ways_writer.writerow(el['way'])
                        way_nodes_writer.writerows(el['way_nodes'])
                        way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=True)


# Importing sqlite so that we can use the csv files in the database

# In[97]:

import sqlite3
import pandas as pd
import numpy as np
from pprint import pprint
from time import time
import csv
#creating a database
sql_file = "hyderabad_india.db"

conn = sqlite3.connect(sql_file)
cur = conn.cursor()
print (type(cur))


# In[98]:

#creating the table nodes
cur.execute('Drop table nodes;')
cur.execute("CREATE TABLE nodes(id,lat,lon,user,uid,version,changeset,timestamp);")
with open ('nodes.csv' , 'rb') as f:
    dic = csv.DictReader(f)
    to_db = [(i['id'],i['lat'],i['lon'],i['user'].decode("utf-8"),i['uid'].decode("utf-8"),i['version'],i['changeset'],i['timestamp'])
            for i in dic]
cur.executemany("INSERT INTO nodes(id,lat,lon,user,uid,version,changeset,timestamp) VALUES (?,?,?,?,?,?,?,?);" , to_db)
conn.commit()   


# In[65]:

##creating the table ways
cur.execute('Drop table if exists ways;')
cur.execute("CREATE TABLE ways(id,user,uid,version,changeset,timestamp);")
with open ('ways.csv' , 'rb') as f:
    dic = csv.DictReader(f)
    to_db = [(i['id'],i['user'].decode("utf-8"),i['uid'].decode("utf-8"),i['version'],i['changeset'],i['timestamp'])
            for i in dic]
cur.executemany("INSERT INTO ways(id,user,uid,version,changeset,timestamp) VALUES (?,?,?,?,?,?);" , to_db)
conn.commit()   


# In[66]:

#creating the table nodes_tags
cur.execute('Drop table if exists nodes_tags;')
cur.execute("CREATE TABLE nodes_tags(id,key,value,type);")
with open ('nodes_tags.csv' , 'rb') as f:
    dic = csv.DictReader(f)
    to_db = [(i['id'].decode("utf-8"),i['key'].decode("utf-8"),i['value'].decode("utf-8"),i['type'].decode("utf-8"))
            for i in dic]
cur.executemany("INSERT INTO nodes_tags(id,key,value,type) VALUES (?,?,?,?);" , to_db)
conn.commit()   


# In[67]:

#creating the table ways_nodes
cur.execute('Drop table if exists ways_nodes;')
cur.execute("CREATE TABLE ways_nodes(id,node_id,position);")
with open ('ways_nodes.csv' , 'rb') as f:
    dic = csv.DictReader(f)
    to_db = [(i['id'],i['node_id'],i['position'])
            for i in dic]
cur.executemany("INSERT INTO ways_nodes(id,node_id,position) VALUES (?,?,?);" , to_db)
conn.commit()   


# In[68]:

#creating the table ways_tags
cur.execute('Drop table if exists ways_tags;')
cur.execute("CREATE TABLE ways_tags(id,key,value,type);")
with open ('ways_tags.csv' , 'rb') as f:
    dic = csv.DictReader(f)
    to_db = [(i['id'].decode("utf-8"),i['key'].decode("utf-8"),i['value'].decode("utf-8"),i['type'].decode("utf-8"))
            for i in dic]
cur.executemany("INSERT INTO ways_tags(id,key,value,type) VALUES (?,?,?,?);" , to_db)
conn.commit()   


# In[118]:

#number of nodes

QUERY ='''SELECT count(*)as num from nodes'''
cur.execute(QUERY)
all_nodes = cur.fetchall()
df = pd.DataFrame(all_nodes)
print df


# There are 3227936 nodes.

# In[120]:

#number of ways tags
QUERY ='''SELECT count(*)as num from ways'''
cur.execute(QUERY)
all_ways = cur.fetchall()
df = pd.DataFrame(all_ways)
print df



# There are 770099 way tags.

# In[122]:

QUERY = '''SELECT DISTINCT(user) 
FROM (SELECT user from nodes UNION SELECT user from ways)
GROUP BY user
ORDER BY count(user) 
DESC
;

'''
cur.execute(QUERY)
top_unique_users = cur.fetchall()
df = pd.DataFrame(top_unique_users)
print "The number of distinct users:" , len(df)


# In[105]:

# number of amenities
QUERY =''' SELECT value, COUNT(*) as num 
FROM (select value , key from nodes_tags UNION ALL select value, key from ways_tags)
WHERE key='amenity'
GROUP BY value
ORDER BY num DESC
;
'''
cur.execute(QUERY)
all_amenities = cur.fetchall()
df = pd.DataFrame(all_amenities)
print "total number of amenities:", len(df)



# In[106]:

#top 10 amenities
QUERY =''' SELECT value, COUNT(*) as num 
FROM (select value , key from nodes_tags UNION ALL select value, key from ways_tags)
WHERE key='amenity'
GROUP BY value
ORDER BY num DESC
limit 10
;
'''
cur.execute(QUERY)
all_amenities = cur.fetchall()
df = pd.DataFrame(all_amenities)
print df


# place of worship tops the list with 387 followed by restaurant with 282

# In[84]:

#number of users
QUERY =''' SELECT user,count(user) from( select user from nodes UNION ALL select user from ways)
GROUP BY user
ORDER BY count(user)
DESC;
'''
cur.execute(QUERY)
all_unique_users = cur.fetchall()
import pandas as pd
df = pd. DataFrame(all_unique_users)
print df[1].describe()
print('\n')
print "Total users:" , df[1].sum()



# The total number of users are 3998035
# The average number of posts are 3986
# 

# In[93]:

#top 5 cuisisnes
QUERY =''' SELECT value, COUNT(*) as num 
FROM (select value , key from nodes_tags UNION ALL select value, key from ways_tags)
WHERE key='cuisine'
GROUP BY value
ORDER BY num DESC
LIMIT 5;

'''
cur.execute(QUERY)
all_cuisines = cur.fetchall()
df = pd.DataFrame(all_cuisines)
print df


# In[128]:

#number of places in hyderabad
QUERY ='''SELECT tags.value, COUNT(*) as count 
FROM (SELECT * FROM nodes_tags UNION ALL 
      SELECT * FROM ways_tags) tags
WHERE tags.key LIKE '%city'
GROUP BY tags.value
ORDER BY count DESC;'''

cur.execute(QUERY)
all_cITIES = cur.fetchall()
df = pd.DataFrame(all_cITIES)
print "number of places :", len(df)


# In[111]:

#most followed religion
QUERY = '''SELECT nodes_tags.value, COUNT(*) as num
FROM nodes_tags 
    JOIN (SELECT DISTINCT(id) FROM nodes_tags WHERE value='place_of_worship') i
    ON nodes_tags.id=i.id
WHERE nodes_tags.key='religion'
GROUP BY nodes_tags.value
ORDER BY num DESC
LIMIT 1;'''
cur.execute(QUERY)
religion = cur.fetchall()
df = pd.DataFrame(religion)
print df


# Advantages and Disadvantages : 
# The main advantage is that Open Street Map data is open-source and therefore free to use. This means anyone can use the data to create their own maps (and then use services like Map Box to generate and host customised map tiles). This means the developer doesn't have to work within Google's constraints. 
# The only imaginable downside to me is quality. Get me right, 99% is the good stuff, but as all crowd sourced data it's hard to maintain consistent quality control. Of course is free to alter and complete the data, but there's no guarantees as there would with a company behind it. 
# 
# 

# Conclusion: 
# After series of iterations in Auditing process, I believe that the data has been cleaned precisely and analysed well in exploration phase .
# 
