import numpy as np
from data.getData_func import *

### Case - 3 ###
'''
user input for:
1) years to simulate
2) which h2 demand scenario
3) freq resolution in 1 year simulation - current: 24 hours / daily
4) discount rate for generators capital costs calculation
5) which h2 pipeline connection configuration (applicable for Case 3 only)
'''

years = '2030'  # [2030] or [2040] or [2050]
h2_scenario_demand = 'TN-H2-G'  # subset of {'TN-H2-G', 'TN-PtG-PtL', 'TN-Strom'}
freq = '4'
discount_rate = 0.07

'''
choose configuration of h2 pipelines connection:
1) 'short' - buses which have h2 demand (which is h2 buses), will connect to any h2 buses in the shortest distance
2) 'all' - each h2 buses will connect to all other h2 buses regardless of short/long distances
3) 'short_fnb_2030' - connects using 'short' config first and then follows roughly similar to proposed h2 pipeline
                    connection based on FNB gas network development plan 2020 - 2030
'''
h2_pipe_config = 'short_fnb_2030'

### Case - 3 ###

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

techno_econ_data = get_techno_econ_data(Nyears, years, discount_rate, network)

pmaxpu_generators = network.generators[
    (network.generators['carrier'] == 'Solar') |
    (network.generators['carrier'] == 'Wind_Offshore') |
    (network.generators['carrier'] == 'Wind_Onshore')]

network.generators_t.p_max_pu = network.generators_t.p_max_pu.reindex(columns=pmaxpu_generators.index)

network.generators_t.p_max_pu.loc[:, pmaxpu_generators.index] = pd.DataFrame(index=network.snapshots,
                                                                             columns=pmaxpu_generators.index,
                                                                             data=np.random.rand(len(network.snapshots),
                                                                                                 len(pmaxpu_generators)))

h2_data = get_hydrogen_data(h2_scenario_demand, years, h2_pipe_config, network)

# connect between electrical buses and hydrogen bus via link (as electrolysis unit)

df_h2_buses_load = pd.DataFrame(h2_data['h2_buses_load'])
df_h2_pipes = pd.DataFrame(h2_data['h2_pipelines'])

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

h2_links = [s + '_Electrolysis' for s in h2_buses_names]

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

h2_stores = [y + '_H2_Store' for y in h2_buses_names]

network.madd('Store',
             h2_stores,
             bus=h2_buses,
             carrier='H2',
             e_nom_extendable=True)

h2_loads = [z + '_H2_Load' for z in h2_buses_names]

network.madd('Load',
             h2_loads,
             bus=h2_buses,
             carrier='H2',
             x=list(df_h2_buses_load['x']),
             y=list(df_h2_buses_load['y'])
             )

ac_loads = network.loads[(network.loads['carrier'] == 'AC')]

ac_loads_p_set = pd.DataFrame(index=network.snapshots,
                              columns=ac_loads.index,
                              data=1000 * np.random.rand(len(network.snapshots), len(ac_loads)))

df_h2_p_set = pd.DataFrame(index=network.snapshots, columns=h2_loads)

for i_load in range(len(df_h2_p_set.columns)):
    df_h2_p_set['{}'.format(df_h2_p_set.columns[i_load])] = df_h2_buses_load['h2_load'][i_load] / len(network.snapshots)

network.loads_t.p_set = pd.merge(ac_loads_p_set, df_h2_p_set, left_index=True, right_index=True)

print('view dataframe for debug')

network.lopf(pyomo=False, solver_name='gurobi')

print('view dataframe for debug')
print('view dataframe for debug')
