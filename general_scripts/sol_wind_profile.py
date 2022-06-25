import pandas as pd

solar_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/electrical/wind_solar_profile/solar_profile_2019.xlsx")
wind_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/electrical/wind_solar_profile/wind_profile_2019.xlsx")

solar_profile = []
wind_profile = []
i_count = 0

for i in range(1, 8761):
    if i % 24 == 0:
        if i == 8761:
            i = 8760
        solar_profile.append(round(solar_data['DE'].iloc[i_count:i].sum() / 24, 5))
        wind_profile.append(round(wind_data['DE'].iloc[i_count:i].sum() / 24, 5))
        i_count = i
