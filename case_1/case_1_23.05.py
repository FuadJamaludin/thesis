from typing import List, Union, Any

import pypsa
import numpy as np
import pandas as pd
from pypsa.linopt import get_var, linexpr, define_constraints
from geopy.geocoders import Nominatim
from geopy import distance


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

    for column_count in df_ac_loads_h2_loads_dist.columns:
        for distance_count in range(len(df_ac_loads_h2_loads_dist[column_count])):
            if df_ac_loads_h2_loads_dist[column_count][distance_count] == df_ac_loads_h2_loads_dist[column_count].min():
                ac_loads_h2_links.append(df_ac_loads_h2_loads_dist.index[distance_count])

    ac_loads_h2_links = list(dict.fromkeys(ac_loads_h2_links))

    dict_h2_data = {'h2_links': ac_loads_h2_links, 'h2_demand_value': round(sum(df_h2_demand['demand_value']) * 1e6, 2)}

    return dict_h2_data


# choose which year to simulate

# years = [2030]
# years = [2040]
years = [2050]

# choose which hydrogen demand scenario to simulate

h2_scenario_demand = "TN-H2-G"
# h2_scenario_demand = "TN-PtG-PtL"
# h2_scenario_demand = "TN-PtG-PtL"

freq = "24"

network = pypsa.Network(get_electrical_data(years))

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

# Nyears = network.snapshot_weightings.objective.sum() / 8760
# Nyears

pmaxpu_generators = network.generators[
    (network.generators['carrier'] == 'Solar') | (network.generators['carrier'] == 'Wind_Offshore') | (
            network.generators['carrier'] == 'Wind_Onshore')]

network.generators_t.p_max_pu = network.generators_t.p_max_pu.reindex(columns=pmaxpu_generators.index)

network.generators_t.p_max_pu.loc[:, pmaxpu_generators.index] = pd.DataFrame(index=network.snapshots,
                                                                             columns=pmaxpu_generators.index,
                                                                             data=np.random.rand(len(network.snapshots),
                                                                                                 len(pmaxpu_generators)))

# connect between electrical buses and hydrogen bus via link (as electrolysis unit)

network.add('Bus', 'Hydrogen', carrier='Hydrogen', x=8.5, y=49.0)

h2_data = get_hydrogen_data(h2_scenario_demand, years)
link_buses = h2_data['h2_links']

link_names = [s + ' Electrolysis' for s in link_buses]

electrolysis_cap_cost = 0
electrolysis_efficiency = 0

if years == [2030]:
    electrolysis_cap_cost = 1886
    electrolysis_efficiency = 0.68
elif years == [2040]:
    electrolysis_cap_cost = 1238.41
    electrolysis_efficiency = 0.72
elif years == [2040]:
    electrolysis_cap_cost = 1012.85
    electrolysis_efficiency = 0.75

network.madd('Link',
             link_names,
             carrier='Hydrogen',
             capital_cost=electrolysis_cap_cost,
             p_nom_extendable=True,
             bus0=link_buses,
             bus1='Hydrogen',
             efficiency=electrolysis_efficiency)

network.add('Store', 'Store Hydrogen', bus='Hydrogen', carrier='Hydrogen', e_nom_extendable=True)


def hydrogen_constraints(n, snapshots):
    electrolysis_index = n.links.query('carrier == "Hydrogen"').index
    electrolysis_vars = get_var(n, 'Link', 'p').loc[n.snapshots[:], electrolysis_index]
    lhs = linexpr((1, electrolysis_vars)).sum().sum()
    h2_value_data = get_hydrogen_data(h2_scenario_demand, years)['h2_demand_value']
    total_production = h2_value_data

    define_constraints(n, lhs, '>=', total_production, 'Link', 'global_hydrogen_production_goal')


def extra_functionality(n, snapshots):
    hydrogen_constraints(n, snapshots)


network.lopf(extra_functionality=extra_functionality, pyomo=False, solver_name='gurobi')

# network.generators.p_nom_opt

# network.generators_t.p

# network.generators.p_nom_opt.plot.bar(ylabel='MW', figsize=(15, 10))
# plt.tight_layout()
