import pandas as pd

ac_data = pd.read_excel("C:/Users/work/pypsa_thesis/data/electrical/ac_profile/ac_gen_20210101_20211231_transnetBW.xlsx")
date_time_list = ac_data['Date'] + ' ' + ac_data['Time of day']
ac_data.insert(0, 'timestamp', date_time_list)
ac_data['timestamp'] = pd.to_datetime(ac_data['timestamp'])
ac_data.drop(['Date', 'Time of day'], axis=1, inplace=True)
ac_data = ac_data.set_index(pd.to_datetime(ac_data['timestamp']))
ac_data_daily = ac_data.resample('D').sum()

for col in list(ac_data_daily.columns):
    ac_data_daily[col] = ac_data_daily[col].div(24)  # MWh to MW

print('test')