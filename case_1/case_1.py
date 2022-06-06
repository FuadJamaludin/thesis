import numpy as np
from data.getData_func import *
from pypsa.linopt import get_var, linexpr, define_constraints

### Case - 1 ###
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

### Case - 1 ###

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

techno_econ_data = get_techno_econ_data(Nyears, years, discount_rate, network)

# append capital costs, marginal costs, efficiency and co2 emissions into network generators, storage_units and carriers
# from techno_econ_data

# capital costs, marginal costs, efficiency for generators
for x_carrier in list(techno_econ_data.index):
    for y_carrier, y_loc in zip(list(network.generators['carrier']), list(network.generators.index)):
        if x_carrier == y_carrier:
            cap_cost_x = techno_econ_data.at['{}'.format(x_carrier), 'capital_costs']
            mar_cost_x = techno_econ_data.at['{}'.format(x_carrier), 'marginal_costs']
            gen_efficiency_x = techno_econ_data.at['{}'.format(x_carrier), 'efficiency']
            network.generators.at['{}'.format(y_loc), 'capital_cost'] = cap_cost_x
            network.generators.at['{}'.format(y_loc), 'marginal_cost'] = mar_cost_x
            network.generators.at['{}'.format(y_loc), 'efficiency'] = gen_efficiency_x

# capital costs, marginal costs, efficiency for storage units
for p_carrier in list(techno_econ_data.index):
    for q_carrier, q_loc in zip(list(network.storage_units['carrier']), list(network.storage_units.index)):
        if p_carrier == q_carrier:
            cap_cost_p = techno_econ_data.at['{}'.format(p_carrier), 'capital_costs']
            mar_cost_p = techno_econ_data.at['{}'.format(p_carrier), 'marginal_costs']
            gen_efficiency_p = techno_econ_data.at['{}'.format(p_carrier), 'efficiency']
            network.storage_units.at['{}'.format(q_loc), 'capital_cost'] = cap_cost_p
            network.storage_units.at['{}'.format(q_loc), 'marginal_cost'] = mar_cost_p
            network.storage_units.at['{}'.format(q_loc), 'efficiency'] = gen_efficiency_p

# co2 emissions for each carriers
for r_carrier in list(techno_econ_data.index):
    for s_carrier in list(network.carriers.index):
        if r_carrier == s_carrier:
            co2_emi = techno_econ_data.at['{}'.format(r_carrier), 'co2_emissions']
            network.carriers.at['{}'.format(s_carrier), 'co2_emissions'] = co2_emi

# current limitation #1: generates random p_max_pu values for renewable generators:
# Solar, Wind Onshore and Wind Offshore

pmaxpu_generators = network.generators[
    (network.generators['carrier'] == 'Solar') |
    (network.generators['carrier'] == 'Wind_Offshore') |
    (network.generators['carrier'] == 'Wind_Onshore')]

network.generators_t.p_max_pu = network.generators_t.p_max_pu.reindex(columns=pmaxpu_generators.index)

network.generators_t.p_max_pu.loc[:, pmaxpu_generators.index] = pd.DataFrame(index=network.snapshots,
                                                                             columns=pmaxpu_generators.index,
                                                                             data=np.random.rand(len(network.snapshots),
                                                                                                 len(pmaxpu_generators)))

# current limitation #2: generates random AC loads/demand for Electrical Buses/Nodes

network.loads_t.p_set = pd.DataFrame(index=network.snapshots,
                                     columns=network.loads.index,
                                     data=1000 * np.random.rand(len(network.snapshots), len(network.loads)))

# calls get_hydrogen_data function to:
# acquire H2 demand data based on chosen H2 scenario demand 'h2_scenario_demand' and 'years' to simulate
# builds H2 pipeline configuration based on chosen H2 pipeline configuration 'h2_pipe_config

h2_data = get_hydrogen_data(h2_scenario_demand, years, h2_pipe_config, network)

# builds and connects H2 network with Electrical Buses/Nodes network
# creates H2 bus

network.add('Bus', 'Hydrogen', carrier='Hydrogen', x=8.5, y=49.0)

link_buses = h2_data['h2_links']

link_names = [s + '_Electrolysis' for s in link_buses]

# electrolysis capital cost and efficiency are based on DEA agency data and pypsa methodology calculations

electrolysis_cap_cost = techno_econ_data.at['Electrolysis', 'capital_costs']
electrolysis_efficiency = techno_econ_data.at['Electrolysis', 'efficiency']

# electrolysis_cap_cost = 0
# electrolysis_efficiency = 1

# connects Electrical Buses/Nodes with H2 Bus using Electrolysis Links

network.madd('Link',
             link_names,
             carrier='Hydrogen',
             capital_cost=electrolysis_cap_cost,
             p_nom_extendable=True,
             bus0=link_buses,
             bus1='Hydrogen',
             efficiency=electrolysis_efficiency)

# attach H2 Store to H2 Bus

network.add('Store', 'Store_Hydrogen', bus='Hydrogen', carrier='Hydrogen', e_nom_extendable=True)

# inserts H2 total demand based on Fraunhofer data into the H2 constraint function

def hydrogen_constraints(n, snapshots):
    electrolysis_index = n.links.query('carrier == "Hydrogen"').index
    electrolysis_vars = get_var(n, 'Link', 'p').loc[n.snapshots[:], electrolysis_index]
    lhs = linexpr((1, electrolysis_vars)).sum().sum()
    total_production = h2_data['h2_demand_value_total']

    define_constraints(n, lhs, '>=', total_production, 'Link', 'global_hydrogen_production_goal')


def extra_functionality(n, snapshots):
    hydrogen_constraints(n, snapshots)


network.lopf(extra_functionality=extra_functionality, pyomo=False, solver_name='gurobi')

print('view dataframe for debug')
print('view dataframe for debug')
