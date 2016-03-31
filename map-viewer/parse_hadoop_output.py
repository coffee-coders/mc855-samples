import sys
import json

color = 0x000000
crime_color = {}
data = {}
for line in sys.stdin:
    key = line.split('\t')[0]
    values = line.split('\t')[1].split('_');

    if key not in crime_color:
        crime_color[key] = { 'color': str(color), 'count': 0 }
        color += 0x0000FF

    data[key + str(crime_color[key]['count'])] = { 'center': {'lat':  values[0], 'lng': values[1]}, \
    'radius': '400', 'color': crime_color[key]['color'] }
    crime_color[key]['count'] += 1

print json.dumps(data)
file_ = open('data.json', 'w')
file_.write(json.dumps(data))
file_.close()
