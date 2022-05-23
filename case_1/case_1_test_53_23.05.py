import pypsa
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
import cartopy.crs as ccrs
from pypsa.linopt import get_var, linexpr, join_exprs, define_constraints
from pypsa.linopf import network_lopf, ilopf, prepare_lopf

csv_folder_name = "C:/Users/work/pypsa/csv_testfiles/1_case_1_test_53"

network = pypsa.Network(csv_folder_name)

years = [2030]
freq = "24"

snapshots = pd.DatetimeIndex([])
for year in years:
    period = pd.date_range(start='{}-01-01 00:00'.format(year),
                           freq='{}H'.format(freq),
                           periods=8760/float(freq))
    snapshots = snapshots.append(period)

network.snapshots = pd.MultiIndex.from_arrays([snapshots.year, snapshots])

network.snapshots

network.loads_t.p_set = pd.DataFrame(index=network.snapshots,
                                     columns=network.loads.index,
                                     data=100*np.random.rand(len(network.snapshots), len(network.loads)))

#network.loads_t.p_set

Nyears = network.snapshot_weightings.objective.sum() / 8760
Nyears

pmaxpu_generators = network.generators[(network.generators['carrier'] == 'Solar') | (network.generators['carrier'] == 'Wind_Offshore') | (network.generators['carrier'] == 'Wind_Onshore')]

network.generators_t.p_max_pu = network.generators_t.p_max_pu.reindex(columns=pmaxpu_generators.index)

network.generators_t.p_max_pu.loc[:,pmaxpu_generators.index] = pd.DataFrame(index=network.snapshots,
                                                                          columns=pmaxpu_generators.index,
                                                                          data=np.random.rand(len(network.snapshots), len(pmaxpu_generators)))

#network.generators_t.p_max_pu

#connect between electrical buses and hydrogen bus via link (as electrolysis unit)

network.add('Bus', 'Hydrogen', carrier='Hydrogen', x=8.5, y=49.0)

link_buses = ['Daxlanden_110kV', 'GKMannheim_110kV', 'Leimen_110kV', 'Oberwald_110kV', 'Kuppenheim_110kV', 'Weier_110kV',
             'Lorrach_110kV', 'Kuhmoos_110kV', 'Ravensburg_110kV', 'Leutkirch_110kV', 'Ehingen_110kV', 'Schmiechen_110kV',
             'SoflingenUlm_110kV', 'Giengen_110kV', 'Aalen_110kV', 'Kupferzell_110kV', 'Hopfingen_110kV', 'Adelsheim_110kV',
             'Grossgartach_110kV', 'Metzingen_110kV', 'Dotternhausen_110kV']

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

bus_colors2 = pd.Series("blue",network.buses.index)
bus_colors2["Hydrogen"]="green"
network.plot(bus_sizes=0.0005, bus_colors=bus_colors2, color_geomap=True)
plt.tight_layout()


# case 1

def hydrogen_constraints(n, snapshots):
    electrolysis_index = n.links.query('carrier == "Hydrogen"').index
    electrolysis_vars = get_var(n, 'Link', 'p').loc[n.snapshots[:], electrolysis_index]
    lhs = linexpr((1, electrolysis_vars)).sum().sum()
    total_production = 13940000  # 13.94 TWh/a = 13940000 MWh/a of H2 demand in BW

    define_constraints(n, lhs, '>=', total_production, 'Link', 'global_hydrogen_production_goal')


def extra_functionality(n, snapshots):
    hydrogen_constraints(n, snapshots)

network.lopf(extra_functionality=extra_functionality,pyomo=False,solver_name='gurobi')

network.generators.p_nom_opt

network.generators_t.p

network.generators.p_nom_opt.plot.bar(ylabel='MW', figsize=(15,10))
plt.tight_layout()
