import numpy as np
from data.getData_func import *
from pypsa.linopt import get_var, linexpr, define_constraints

### Case - 1 ###
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

### Case - 1 ###

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

network.loads_t.p_set = pd.DataFrame(index=network.snapshots,
                                     columns=network.loads.index,
                                     data=1000 * np.random.rand(len(network.snapshots), len(network.loads)))

h2_data = get_hydrogen_data(h2_scenario_demand, years, h2_pipe_config, network)

# connect between electrical buses and hydrogen bus via link (as electrolysis unit)

network.add('Bus', 'Hydrogen', carrier='Hydrogen', x=8.5, y=49.0)

link_buses = h2_data['h2_links']

link_names = [s + '_Electrolysis' for s in link_buses]

# electrolysis capital cost and efficiency are based on DEA agency data and pypsa methodology calculations

electrolysis_cap_cost = techno_econ_data.at['Electrolysis', 'capital_costs']
electrolysis_efficiency = techno_econ_data.at['Electrolysis', 'efficiency']

network.madd('Link',
             link_names,
             carrier='Hydrogen',
             capital_cost=electrolysis_cap_cost,
             p_nom_extendable=True,
             bus0=link_buses,
             bus1='Hydrogen',
             efficiency=electrolysis_efficiency)

network.add('Store', 'Store_Hydrogen', bus='Hydrogen', carrier='Hydrogen', e_nom_extendable=True)


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
