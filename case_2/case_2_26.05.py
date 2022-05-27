import numpy as np
import pandas as pd
import pypsa
from geopy import distance
from geopy.geocoders import Nominatim



def get_electrical_data(years_elect):
    if years_elect == [2030]:
        return "C:/Users/work/pypsa_thesis/data/electrical/1_case_1_2030"
    elif years_elect == [2040]:
        return "C:/Users/work/pypsa_thesis/data/electrical/2_case_1_2040"
    elif years_elect == [2050]:
        return "C:/Users/work/pypsa_thesis/data/electrical/3_case_1_2050"


def get_hydrogen_data(scenario_h2, years_h2):
    if scenario_h2 == 'TN-H2-G':
        if years_h2 == [2030]:
            load_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-H2-G/BW_2030.xlsx",
                                      index_col=0)

        elif years_h2 == [2040]:
            load_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-H2-G/BW_2040.xlsx",
                                      index_col=0)

        elif years_h2 == [2050]:
            load_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-H2-G/BW_2050.xlsx",
                                      index_col=0)

    elif scenario_h2 == 'TN-PtG-PtL':
        if years_h2 == [2030]:
            load_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-PtG-PtL/BW_2030.xlsx",
                                      index_col=0)

        elif years_h2 == [2040]:
            load_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-PtG-PtL/BW_2040.xlsx",
                                      index_col=0)

        elif years_h2 == [2050]:
            load_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-PtG-PtL/BW_2050.xlsx",
                                      index_col=0)

    elif scenario_h2 == 'TN-Strom':
        if years_h2 == [2030]:
            load_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-Strom/BW_2030.xlsx",
                                      index_col=0)

        elif years_h2 == [2040]:
            load_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-Strom/BW_2040.xlsx",
                                      index_col=0)

        elif years_h2 == [2050]:
            load_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-Strom/BW_2050.xlsx",
                                      index_col=0)

    df_h2_demand = pd.DataFrame(load_data)
    df_h2_demand.index.names = ['location_name']
    df_h2_demand.reset_index(inplace=True)

    for loc_count in range(len(df_h2_demand['location_name'])):
        geolocator = Nominatim(user_agent="locate_h2_demand")
        locate_h2_demand = geolocator.geocode(df_h2_demand['location_name'][loc_count].split(',')[0])
        df_h2_demand['x'][loc_count] = locate_h2_demand.longitude
        df_h2_demand['y'][loc_count] = locate_h2_demand.latitude

    df_ac_loads_h2_loads_dist = pd.DataFrame(index=network.loads.index, columns=df_h2_demand['location_name'])

    for city_count_x in range(len(network.loads.index)):
        for city_count_y in range(len(df_h2_demand['location_name'])):
            if network.loads.index[city_count_x] != df_h2_demand['location_name'][city_count_y]:
                city_1 = (network.loads['y'][city_count_x], network.loads['x'][city_count_x])
                city_2 = (df_h2_demand['y'][city_count_y], df_h2_demand['x'][city_count_y])
                dist_city1_city2 = distance.distance(city_1, city_2).km
                df_ac_loads_h2_loads_dist.at[
                    network.loads.index[city_count_x], df_h2_demand['location_name'][city_count_y]] = dist_city1_city2

    ac_loads_h2_links = []

    for column_count_x in df_ac_loads_h2_loads_dist.columns:
        for distance_count_x in range(len(df_ac_loads_h2_loads_dist[column_count_x])):
            if df_ac_loads_h2_loads_dist[column_count_x][distance_count_x] == \
                    df_ac_loads_h2_loads_dist[column_count_x].min():
                ac_loads_h2_links.append(df_ac_loads_h2_loads_dist.index[distance_count_x])

    ac_loads_h2_links = list(dict.fromkeys(ac_loads_h2_links))

    df_h2_buses_load = pd.DataFrame(index=ac_loads_h2_links, columns={'h2_load': [], 'x': [], 'y': []})

    for buses_count in range(len(network.buses.index)):
        for h2_buses_count in range(len(df_h2_buses_load.index)):
            if network.buses.index[buses_count] == df_h2_buses_load.index[h2_buses_count]:
                df_h2_buses_load['x'][h2_buses_count] = network.buses['x'][buses_count]
                df_h2_buses_load['y'][h2_buses_count] = network.buses['y'][buses_count]

    df_h2_buses_load.fillna(0, inplace=True)

    for column_count_y, i_count_y in zip(df_ac_loads_h2_loads_dist.columns, range(len(df_h2_demand['location_name']))):
        for distance_count_y in range(len(df_ac_loads_h2_loads_dist[column_count_y])):
            if df_ac_loads_h2_loads_dist[column_count_y][distance_count_y] == \
                    df_ac_loads_h2_loads_dist[column_count_y].min():
                h2_load_value = df_h2_demand[df_h2_demand['location_name'] == column_count_y]['demand_value'][
                                    i_count_y] * 1e6  # in MWh
                h2_demand_loc = df_ac_loads_h2_loads_dist.index[distance_count_y]
                if df_h2_buses_load.at[h2_demand_loc, 'h2_load'] == 0:
                    df_h2_buses_load.at[h2_demand_loc, 'h2_load'] = h2_load_value
                else:
                    df_h2_buses_load.at[h2_demand_loc, 'h2_load'] = df_h2_buses_load.at[h2_demand_loc, 'h2_load'] + \
                                                                    h2_load_value

    dict_h2_data = {'h2_links': ac_loads_h2_links,
                    'h2_dataframe': df_h2_demand,
                    'h2_buses_load': df_h2_buses_load,
                    'h2_demand_value_total': round(sum(df_h2_demand['demand_value']) * 1e6, 2)}  # in MWh

    return dict_h2_data


# choose which year to simulate

years = [2030]  # [2030] or [2040] or [2050]

# choose which hydrogen demand scenario to simulate

h2_scenario_demand = "TN-H2-G"  # "TN-H2-G" or "TN-PtG-PtL" or "TN-Strom"

freq = "24"

network = pypsa.Network(get_electrical_data(years))

snapshots = pd.DatetimeIndex([])
for year in years:
    period = pd.date_range(start='{}-01-01 00:00'.format(year),
                           freq='{}H'.format(freq),
                           periods=8760 / float(freq))
    snapshots = snapshots.append(period)

network.snapshots = pd.MultiIndex.from_arrays([snapshots.year, snapshots])

# network.snapshots

'''

Nyears value depends on the snapshot resolution freq variable
current freq = 24 with Nyears value of = 0.041666666666666664
Change of Nyears value will affect the calculation of capital cost using pypsa-eur methodology from 
the add_electricity script

costs["capital_cost"] = ((annuity(costs["lifetime"], costs["discount rate"]) + 
                            costs["FOM"]/100.) *
                            costs["investment"] * Nyears)
                            
'''
# Nyears = network.snapshot_weightings.objective.sum() / 8760
# Nyears

pmaxpu_generators = network.generators[
    (network.generators['carrier'] == 'Solar') |
    (network.generators['carrier'] == 'Wind_Offshore') |
    (network.generators['carrier'] == 'Wind_Onshore')]

network.generators_t.p_max_pu = network.generators_t.p_max_pu.reindex(columns=pmaxpu_generators.index)

network.generators_t.p_max_pu.loc[:, pmaxpu_generators.index] = pd.DataFrame(index=network.snapshots,
                                                                             columns=pmaxpu_generators.index,
                                                                             data=np.random.rand(len(network.snapshots),
                                                                                                 len(pmaxpu_generators)))

h2_data = get_hydrogen_data(h2_scenario_demand, years)

# connect between electrical buses and hydrogen bus via link (as electrolysis unit)

df_h2_buses_load = pd.DataFrame(h2_data['h2_buses_load'])

h2_buses_names = list(df_h2_buses_load.index)

h2_buses = [x + '_Hydrogen' for x in h2_buses_names]

network.madd('Bus',
             h2_buses,
             carrier='Hydrogen',
             x=list(df_h2_buses_load['x']),
             y=list(df_h2_buses_load['y'])
             )

# electrolysis capital cost and efficiency are based on DEA agency data and pypsa methodology calculations

electrolysis_cap_cost = 0
electrolysis_efficiency = 0

if years == [2030]:
    electrolysis_cap_cost = 1886
    electrolysis_efficiency = 0.68
elif years == [2040]:
    electrolysis_cap_cost = 1238.41
    electrolysis_efficiency = 0.72
elif years == [2050]:
    electrolysis_cap_cost = 1012.85
    electrolysis_efficiency = 0.75

h2_links = [s + '_Electrolysis' for s in h2_buses_names]

network.madd('Link',
             h2_links,
             carrier='Hydrogen',
             capital_cost=electrolysis_cap_cost,
             p_nom_extendable=True,
             bus0=h2_buses_names,
             bus1=h2_buses,
             efficiency=electrolysis_efficiency)

h2_stores = [y + '_H2_Store' for y in h2_buses_names]

network.madd('Store',
             h2_stores,
             bus=h2_buses,
             carrier='Hydrogen',
             e_nom_extendable=True)

h2_loads = [z + '_H2_Load' for z in h2_buses_names]

network.madd('Load',
             h2_loads,
             bus=h2_buses,
             p_set=list(df_h2_buses_load['h2_load']),
             carrier='Hydrogen',
             x=list(df_h2_buses_load['x']),
             y=list(df_h2_buses_load['y'])
             )

ac_loads = network.loads[(network.loads['carrier'] == 'AC')]

network.loads_t.p_set = pd.DataFrame(index=network.snapshots,
                                     columns=ac_loads.index,
                                     data=100 * np.random.rand(len(network.snapshots), len(ac_loads)))

network.lopf(pyomo=False, solver_name='gurobi')