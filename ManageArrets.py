import os
import re
import json
import requests
import pandas as pd
import numpy as np

# Configuration corrigée
API_KEY = 'tPf57BDIitFBVTylzjw4WMoDN1Z5NnNI'
API_BASE = 'https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring?MonitoringRef='
ARRET_PREFIX = 'STIF:StopArea:SP:'

# Chargement des données
df_stop = pd.read_json('data/arr.json', dtype=str)

# Test avec correction
test_row = df_stop.iloc[132]

# En-têtes corrigés
headers = {
    'Accept': 'application/json',
    'apikey': API_KEY
}

if os.path.exists('data/lines_with_name.json'):
    with open('data/lines_with_name.json', 'r', encoding='utf-8') as f:
        df_lines_no_dups_with_name = pd.read_json(f, dtype=str)
else:
    list_lines = []
    for idx, row in df_stop.iterrows():
        # Do request
        monitoring_ref = f"{ARRET_PREFIX}{row['zdaid']}:"
        url = f"{API_BASE}{monitoring_ref}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # Process data
            monitored_stop_visits = data.get('Siri', {}).get('ServiceDelivery', {}).get('StopMonitoringDelivery', [{}])[0].get('MonitoredStopVisit', [])
            for visit in monitored_stop_visits:
                line_ref = visit.get('MonitoredVehicleJourney', {}).get('LineRef', 'Unknown')
                list_lines.append({
                    'zdaid': row['zdaid'],
                    'name': row['arrname'],
                    'type': row['arrtype'],
                    'town': row['arrtown'],
                    'postal_region': row['arrpostalregion'],
                    'accessibility': row['arraccessibility'],
                    'audiblesignals': row['arraudiblesignals'],
                    'visualsigns': row['arrvisualsigns'],
                    'line_ref': line_ref.get('value', {})
                })
        else:
            print(f"Error {response.status_code}: {response.text}")
            
    print("Finished processing all stops.")

    df_lines = pd.DataFrame(list_lines)
    df_lines_no_dups = df_lines.drop_duplicates().reset_index(drop=True)

    line_data_df = pd.read_json('data/ligne.json', dtype=str)
    line_df = line_data_df.copy()
    # Add  STIF:Line:: prefix and : suffix to id_line
    line_df['id_line'] = 'STIF:Line::' + line_df['id_line'] + ':'

    # Add name_line to df_lines_no_dups
    df_lines_no_dups_with_name = df_lines_no_dups.merge(line_df[['id_line', 'name_line', 'shortname_groupoflines','colourweb_hexa']], left_on='line_ref', right_on='id_line', how='left').drop(columns=['id_line'])

    df_lines_no_dups_with_name.to_json('data/lines_with_name.json', orient='records', force_ascii=False)

# region === BUS LINES ===

bus_df = df_lines_no_dups_with_name[df_lines_no_dups_with_name['type'] == 'bus']
print(f"Number of unique bus lines: {bus_df['name_line'].nunique()}")
print(f"Total stops: {len(bus_df['zdaid'])}")
bus_df = bus_df.sort_values(by='name_line', key=lambda x: x.map(lambda y: [int(c) if c.isdigit() else c for c in re.split('([0-9]+)', y)]))

# For each unique line_ref in bus_df, save in lines_bus.json the list of line_ref, shortname_groupoflines, colourweb_hexa and number of stops
lines_bus = []
for line in bus_df['line_ref'].unique():
    line_info = bus_df[bus_df['line_ref'] == line].iloc[0]
    lines_bus.append({
        'line_ref': line,
        'name_line': line_info['name_line'],
        'shortname_groupoflines': line_info['shortname_groupoflines'],
        'colourweb_hexa': line_info['colourweb_hexa'],
        'number_of_stops': len(bus_df[bus_df['line_ref'] == line])
    })
    
with open('api/bus/lines_bus.json', 'w') as f:
    f.write('---\nlayout: none\npermalink: /api/bus/lines_bus/\n---\n')
    json.dump(lines_bus, f)
    
# For each unique line_ref in bus_df, save in stops_bus.json the list of zdaid, arrname, arrtown, postal_region, accessibility, audiblesignals, visualsigns
for line in bus_df['line_ref'].unique():
    stops = bus_df[bus_df['line_ref'] == line][['zdaid', 'name', 'town', 'postal_region', 'accessibility', 'audiblesignals', 'visualsigns']].to_dict(orient='records')
    stops_bus = {
        'line_ref': line,
        'stops': stops
    }
    print(f"Saving stops for line {line} with {len(stops)} stops.")

    with open(f'api/bus/stops/stops_bus_{line.replace(":", "[")}.json', 'w') as f:
        f.write(f'---\nlayout: none\npermalink: /api/bus/stops/stops_bus_{line}/\n---\n')
        json.dump(stops_bus, f)

# endregion

# region === RAIL LINES ===

rail_df = df_lines_no_dups_with_name[df_lines_no_dups_with_name['type'] == 'rail']
print(f"Number of unique rail lines: {rail_df['name_line'].unique()}")
print(f"Total stops: {len(rail_df['zdaid'])}")
# sort rail_df by name_line
rail_df = rail_df.sort_values(by='name_line')

# For each unique line_ref in rail_df, save in lines_rail.json the list of line_ref, shortname_groupoflines, colourweb_hexa and number of stops
lines_rail = []
for line in rail_df['line_ref'].unique():
    line_info = rail_df[rail_df['line_ref'] == line].iloc[0]
    lines_rail.append({
        'line_ref': line,
        'name_line': line_info['name_line'],
        'shortname_groupoflines': line_info['shortname_groupoflines'],
        'colourweb_hexa': line_info['colourweb_hexa'],
        'number_of_stops': len(rail_df[rail_df['line_ref'] == line])
    })

with open('api/rail/lines_rail.json', 'w') as f:
    f.write('---\nlayout: none\npermalink: /api/rail/lines_rail/\n---\n')
    json.dump(lines_rail, f)
    
# For each unique line_ref in rail_df, save in stops_rail.json the list of zdaid, arrname, arrtown, postal_region, accessibility, audiblesignals, visualsigns
for line in rail_df['line_ref'].unique():
    stops = rail_df[rail_df['line_ref'] == line][['zdaid', 'name', 'town', 'postal_region', 'accessibility', 'audiblesignals', 'visualsigns']].to_dict(orient='records')
    stops_rail = {
        'line_ref': line,
        'stops': stops
    }
    print(f"Saving stops for line {line} with {len(stops)} stops.")

    with open(f'api/rail/stops/stops_rail_{line.replace(":", "[")}.json', 'w') as f:
        f.write(f'---\nlayout: none\npermalink: /api/rail/stops/stops_rail_{line}/\n---\n')
        json.dump(stops_rail, f)

# endregion

# region === METRO LINES ===

metro_df = df_lines_no_dups_with_name[df_lines_no_dups_with_name['type'] == 'metro']
print(f"Number of unique metro lines: {metro_df['name_line'].nunique()}")
print(f"Total stops: {len(metro_df['zdaid'])}")
# sort metro_df by name_line as alphanumeric
def alphanum_key(key):
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', key)]
metro_df = metro_df.sort_values(by='name_line', key=lambda x: x.map(alphanum_key))

# For each unique line_ref in metro_df, save in lines_metro.json the list of line_ref, shortname_groupoflines, colourweb_hexa and number of stops
lines_metro = []
for line in metro_df['line_ref'].unique():
    line_info = metro_df[metro_df['line_ref'] == line].iloc[0]
    lines_metro.append({
        'line_ref': line,
        'name_line': line_info['name_line'],
        'shortname_groupoflines': line_info['shortname_groupoflines'],
        'colourweb_hexa': line_info['colourweb_hexa'],
        'number_of_stops': len(metro_df[metro_df['line_ref'] == line])
    })
    
with open('api/metro/lines_metro.json', 'w') as f:
    f.write('---\nlayout: none\npermalink: /api/metro/lines_metro/\n---\n')
    json.dump(lines_metro, f)
    
# For each unique line_ref in metro_df, save in stops_metro.json the list of zdaid, arrname, arrtown, postal_region, accessibility, audiblesignals, visualsigns
for line in metro_df['line_ref'].unique():
    stops = metro_df[metro_df['line_ref'] == line][['zdaid', 'name', 'town', 'postal_region', 'accessibility', 'audiblesignals', 'visualsigns']].to_dict(orient='records')
    stops_metro = {
        'line_ref': line,
        'stops': stops
    }
    print(f"Saving stops for line {line} with {len(stops)} stops.")

    with open(f'api/metro/stops/stops_metro_{line.replace(":", "[")}.json', 'w') as f:
        f.write(f'---\nlayout: none\npermalink: /api/metro/stops/stops_metro_{line}/\n---\n')
        json.dump(stops_metro, f)

# endregion

# region === TRAM LINES ===
tram_df = df_lines_no_dups_with_name[df_lines_no_dups_with_name['type'] == 'tram']
print(f"Number of unique tram lines: {tram_df['name_line'].nunique()}")
print(f"Total stops: {len(tram_df['zdaid'])}")
# sort tram_df by name_line
tram_df = tram_df.sort_values(by='name_line', key=lambda x: x.map(alphanum_key))

# For each unique line_ref in tram_df, save in lines_tram.json the list of line_ref, shortname_groupoflines, colourweb_hexa and number of stops
lines_tram = []
for line in tram_df['line_ref'].unique():
    line_info = tram_df[tram_df['line_ref'] == line].iloc[0]
    lines_tram.append({
        'line_ref': line,
        'name_line': line_info['name_line'],
        'shortname_groupoflines': line_info['shortname_groupoflines'],
        'colourweb_hexa': line_info['colourweb_hexa'],
        'number_of_stops': len(tram_df[tram_df['line_ref'] == line])
    })
    
with open('api/tram/lines_tram.json', 'w') as f:
    f.write('---\nlayout: none\npermalink: /api/tram/lines_tram/\n---\n')
    json.dump(lines_tram, f)
    
# For each unique line_ref in tram_df, save in stops_tram.json the list of zdaid, arrname, arrtown, postal_region, accessibility, audiblesignals, visualsigns
for line in tram_df['line_ref'].unique():
    stops = tram_df[tram_df['line_ref'] == line][['zdaid', 'name', 'town', 'postal_region', 'accessibility', 'audiblesignals', 'visualsigns']].to_dict(orient='records')
    stops_tram = {
        'line_ref': line,
        'stops': stops
    }
    print(f"Saving stops for line {line} with {len(stops)} stops.")

    with open(f'api/tram/stops/stops_tram_{line.replace(":", "[")}.json', 'w') as f:
        f.write(f'---\nlayout: none\npermalink: /api/tram/stops/stops_tram_{line}/\n---\n')
        json.dump(stops_tram, f)
        
# endregion    
        
        


# Get unique lines from df_lines_no_dups
unique_lines = df_lines_no_dups['line_ref'].unique()
unique_lines_other = line_df['id_line'].unique()

print(f"Unique lines in monitored data: {len(unique_lines)}")
print(f"Unique lines in line data: {len(unique_lines_other)}")
