import numpy as np
from data.getData_func import *

### Case - 3 ###
'''
user input for:
1) years to simulate
2) which h2 demand scenario
3) freq resolution in 1 year simulation - e.g. timesteps of: 24h / 12h / 6h / 1h 
4) annual discount rate of capital costs calculation for generators, storage units, electrolysis and H2 pipelines
5) which h2 pipeline connection configuration (applicable for Case 3 only)
'''

years = '2030'  # subset of {'2030', '2040', '2050'}
h2_scenario_demand = 'TN-H2-G'  # subset of {'TN-H2-G', 'TN-PtG-PtL', 'TN-Strom'}
freq = '24'
discount_rate = 0.07

'''
choose configuration of H2 pipelines connection (applicable for Case 3 only):
1) 'short' - buses which have H2 demand (which are H2 buses), will connect to any H2 buses in the shortest distance
2) 'all' - each H2 buses will connect to all other H2 buses regardless of short/long distances
3) 'short_fnb_2030' - connects using 'short' config first and then follows roughly similar to proposed H2 pipeline
                      connection based on FNB gas network development plan 2020 - 2030. This configuration currently
                      LIMITED ONLY for 'TN-H2-G' H2 scenario demand 
                    
'''
h2_pipe_config = 'short_fnb_2030'

### Case - 3 ###

# get electrical network from network csv files; generators, storage_units, lines, loads & etc.
# create snapshots based on chosen 'years' and timesteps 'freq' to simulate

network = get_network(years)

snapshots = pd.DatetimeIndex([])
period = pd.date_range(start='{}-01-01 00:00'.format(years),
                       freq='{}H'.format(freq),
                       periods=8760 / float(freq))
snapshots = snapshots.append(period)

network.snapshots = pd.MultiIndex.from_arrays([snapshots.year, snapshots])

Nyears = network.snapshot_weightings.objective.sum() / 8760

'''

Nyears value depends on the snapshot resolution freq variable
Change of Nyears value will affect the calculation of capital cost based on pypsa-eur methodology from 
the add_electricity script

Nyears = network.snapshot_weightings.objective.sum() / 8760
Nyears

costs["capital_cost"] = ((annuity(costs["lifetime"], costs["discount rate"]) + 
                            costs["FOM"]/100.) *
                            costs["investment"] * Nyears)

'''
# calls get_techno_econ_data function to calculate capital costs, marginal costs, efficiency for generators,
# storage_units, electrolysis and H2 pipeline
# the function depends on Nyears (changes with the input value of 'freq' timesteps), years, discount rate
# append capital costs, marginal costs, efficiency and co2 emissions into network generators, storage_units and carriers
# the function returns network

techno_econ_data = get_techno_econ_data(Nyears, years, discount_rate, network)

# generates p_max_pu values for renewable generators based on data from open-power-system data repository:
# current p_max_pu snapshots only applicable for 365 days snapshots length (24H freq) - note on 26.06.2022
# Solar, Wind Onshore and Wind Offshore

set_re_profile(network)

# calls get_hydrogen_data function to:
# acquire H2 demand data based on chosen H2 scenario demand 'h2_scenario_demand' and 'years' to simulate
# builds H2 pipeline configuration based on chosen H2 pipeline configuration 'h2_pipe_config

h2_data = get_hydrogen_data(h2_scenario_demand, years, h2_pipe_config, network)

# builds and connects H2 network with Electrical Buses/Nodes network

df_h2_buses_load = pd.DataFrame(h2_data['h2_buses_load'])  # dataframe of H2 demand for each H2 Buses/Loads
df_h2_pipes = pd.DataFrame(h2_data['h2_pipelines'])  # dataframe of H2 pipeline connections between H2 Buses

# creates H2 Buses

h2_buses_names = list(df_h2_buses_load.index)

h2_buses = [x + '_H2_Bus' for x in h2_buses_names]

network.madd('Bus',
             h2_buses,
             carrier='H2',
             x=list(df_h2_buses_load['x']),
             y=list(df_h2_buses_load['y'])
             )

# electrolysis capital cost and efficiency are based on DEA agency data and pypsa methodology calculations

electrolysis_cap_cost = techno_econ_data.at['Electrolysis', 'capital_costs']
electrolysis_efficiency = techno_econ_data.at['Electrolysis', 'efficiency']

# electrolysis_cap_cost = 0
# electrolysis_efficiency = 1

h2_links = [s + '_Electrolysis' for s in h2_buses_names]

# connects Electrical Buses/Nodes with H2 Buses using Electrolysis Links

network.madd('Link',
             h2_links,
             carrier='H2',
             capital_cost=electrolysis_cap_cost,
             p_nom_extendable=True,
             bus0=h2_buses_names,
             bus1=h2_buses,
             efficiency=electrolysis_efficiency)

h2_pipe_cap_cost = techno_econ_data.at['H2_(g)_pipeline', 'capital_costs']
h2_pipe_efficiency = techno_econ_data.at['H2_(g)_pipeline', 'efficiency']

# h2_pipe_cap_cost = 0
# h2_pipe_efficiency = 1

# attach and connect H2 pipelines between the H2 buses

network.madd('Link',
             df_h2_pipes.index,
             bus0=list(df_h2_pipes['bus_0']),
             bus1=list(df_h2_pipes['bus_1']),
             p_min_pu=-1,
             p_nom_extendable=True,
             length=list(df_h2_pipes['distance_km']),
             capital_cost=h2_pipe_cap_cost * df_h2_pipes['distance_km'],
             efficiency=h2_pipe_efficiency,
             carrier='H2')

# attach H2 Stores to H2 Buses

h2_stores = [y + '_H2_Store' for y in h2_buses_names]

network.madd('Store',
             h2_stores,
             bus=h2_buses,
             carrier='H2',
             e_nom_extendable=True)

# attach H2 Loads to H2 Buses

h2_loads = [z + '_H2_Load' for z in h2_buses_names]

# time series AC and H2 load

network.madd('Load',
             h2_loads,
             bus=h2_buses,
             carrier='H2',
             x=list(df_h2_buses_load['x']),
             y=list(df_h2_buses_load['y'])
             )

# current limitation #1: generates random AC loads/demand for Electrical Buses/Nodes

ac_loads = network.loads[(network.loads['carrier'] == 'AC')]

ac_loads_p_set = pd.DataFrame(index=network.snapshots,
                              columns=ac_loads.index,
                              data=1000 * np.random.rand(len(network.snapshots), len(ac_loads)))

# H2 loads set in series, based on Fraunhofer data

df_h2_p_set = pd.DataFrame(index=network.snapshots, columns=h2_loads)

for i_load in range(len(df_h2_p_set.columns)):
    df_h2_p_set['{}'.format(df_h2_p_set.columns[i_load])] = df_h2_buses_load['h2_load'][i_load] / len(network.snapshots)

# merge series of AC loads and H2 loads

network.loads_t.p_set = pd.merge(ac_loads_p_set, df_h2_p_set, left_index=True, right_index=True)

network.lopf(pyomo=False, solver_name='gurobi')


