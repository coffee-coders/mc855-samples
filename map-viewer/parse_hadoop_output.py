import sys
import json
import random

color = 0x000000
crime_color = {}
data = {}
for line in sys.stdin:
    key = line.split('\t')[0]
    values = line.split('\t')[1].split('_')

    r = lambda: random.randint(0,255)

    if key not in crime_color:
        crime_color[key] = { 'color': '#%02X%02X%02X' % (r(),r(),r()), 'count': 0 }

    data[key] = { 'center': {'lat':  float(values[0]), 'lng': float(values[1])}, \
    'radius': 400, 'color': crime_color[key]['color'] }
    crime_color[key]['count'] += 1

file_ = open('data.json', 'w')
file_.write(json.dumps(data))
file_.close()
