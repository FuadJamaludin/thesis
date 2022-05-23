import pypsa
import numpy as np
import pandas as pd
from pypsa.linopt import get_var, linexpr, define_constraints
from geopy.geocoders import Nominatim
from geopy import distance

csv_folder_name = "C:/Users/work/pypsa/csv_testfiles/1_case_1_test_53"

network = pypsa.Network(csv_folder_name)

years = [2030]
freq = "24"

snapshots = pd.DatetimeIndex([])
for year in years:
    period = pd.date_range(start='{}-01-01 00:00'.format(year),
                           freq='{}H'.format(freq),
                           periods=8760 / float(freq))
    snapshots = snapshots.append(period)

network.snapshots = pd.MultiIndex.from_arrays([snapshots.year, snapshots])

network.snapshots

network.loads_t.p_set = pd.DataFrame(index=network.snapshots,
                                     columns=network.loads.index,
                                     data=100 * np.random.rand(len(network.snapshots), len(network.loads)))

# network.loads_t.p_set

Nyears = network.snapshot_weightings.objective.sum() / 8760
Nyears

pmaxpu_generators = network.generators[
    (network.generators['carrier'] == 'Solar') | (network.generators['carrier'] == 'Wind_Offshore') | (
                network.generators['carrier'] == 'Wind_Onshore')]

network.generators_t.p_max_pu = network.generators_t.p_max_pu.reindex(columns=pmaxpu_generators.index)

network.generators_t.p_max_pu.loc[:, pmaxpu_generators.index] = pd.DataFrame(index=network.snapshots,
                                                                             columns=pmaxpu_generators.index,
                                                                             data=np.random.rand(len(network.snapshots),
                                                                                                 len(pmaxpu_generators)))

# network.generators_t.p_max_pu

excel_BW = pd.read_excel('C:/Users/HP Elitebook 840/Desktop/sources/hydrogen/TN-H2-G/BW_del_special_chars.xlsx',
                         index_col=0)
df_BW = pd.DataFrame(excel_BW)
df_BW.reset_index(inplace=True)

for loc_count in range(len(df_BW['NUTS_NAME'])):
    geolocator = Nominatim(user_agent="pycharm")
    location = geolocator.geocode(df_BW['NUTS_NAME'][loc_count].split(',')[0])
    df_BW['x'][loc_count] = location.longitude
    df_BW['y'][loc_count] = location.latitude
    # print(location.address)
    # print((location.latitude, location.longitude))

'''
possible_city_H2_links = []
dict_network_dist_store = {}
for loads_name in list(network.loads.index):
    dict_network_dist_store['{}'.format(loads_name)] = []
'''

df_ac_loads_h2_loads_dist = pd.DataFrame(index=network.loads.index, columns=df_BW['NUTS_NAME'])

for city_count_x in range(len(network.loads.index)):
    for city_count_y in range(len(df_BW['NUTS_NAME'])):
        if network.loads.index[city_count_x] != df_BW['NUTS_NAME'][city_count_y]:
            city_1 = (network.loads['y'][city_count_x], network.loads['x'][city_count_x])
            city_2 = (df_BW['y'][city_count_y], df_BW['x'][city_count_y])
            dist_city1_city2 = distance.distance(city_1, city_2).km
            df_ac_loads_h2_loads_dist.at[network.loads.index[city_count_x], df_BW['NUTS_NAME'][city_count_y]] = dist_city1_city2

ac_loads_H2_links = []

for column_count in df_ac_loads_h2_loads_dist.columns:
    for distance_count in range(len(df_ac_loads_h2_loads_dist[column_count])):
        if df_ac_loads_h2_loads_dist[column_count][distance_count] == df_ac_loads_h2_loads_dist[column_count].min():
            ac_loads_H2_links.append(df_ac_loads_h2_loads_dist.index[distance_count])

ac_loads_H2_links = list(dict.fromkeys(ac_loads_H2_links))

print('hello')

# connect between electrical buses and hydrogen bus via link (as electrolysis unit)

network.add('Bus', 'Hydrogen', carrier='Hydrogen', x=8.5, y=49.0)

link_buses = ac_loads_H2_links

link_names = [s + ' Electrolysis' for s in link_buses]

network.madd('Link',
             link_names,
             carrier='Hydrogen',
             capital_cost=350,
             p_nom_extendable=True,
             bus0=link_buses,
             bus1='Hydrogen',
             efficiency=0.8)

network.add('Store', 'Store Hydrogen', bus='Hydrogen', carrier='Hydrogen', e_nom_extendable=True)

# bus_colors2 = pd.Series("blue",network.buses.index)
# bus_colors2["Hydrogen"]="green"
# network.plot(bus_sizes=0.0005, bus_colors=bus_colors2, color_geomap=True)
# plt.tight_layout()

# case 1

def hydrogen_constraints(n, snapshots):
    electrolysis_index = n.links.query('carrier == "Hydrogen"').index
    electrolysis_vars = get_var(n, 'Link', 'p').loc[n.snapshots[:], electrolysis_index]
    lhs = linexpr((1, electrolysis_vars)).sum().sum()
    total_production = 13940000  # 13.94 TWh/a = 13940000 MWh/a of H2 demand in BW

    define_constraints(n, lhs, '>=', total_production, 'Link', 'global_hydrogen_production_goal')

def extra_functionality(n, snapshots):
    hydrogen_constraints(n, snapshots)


network.lopf(extra_functionality=extra_functionality, pyomo=False, solver_name='gurobi')

network.generators.p_nom_opt

network.generators_t.p

network.generators.p_nom_opt.plot.bar(ylabel='MW', figsize=(15, 10))
# plt.tight_layout()
