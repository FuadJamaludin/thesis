import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim

excel_BW = pd.read_excel('C:/Users/HP Elitebook 840/Desktop/sources/hydrogen/TN-H2-G\BW.xlsx', index_col=0)
df_BW = pd.DataFrame(excel_BW)
df_BW.reset_index(inplace=True)

for loc_count in range(len(df_BW['NUTS_NAME'])):
    geolocator = Nominatim(user_agent="pycharm")
    location = geolocator.geocode(df_BW['NUTS_NAME'][loc_count])
    df_BW['x'][loc_count] = location.longitude
    df_BW['y'][loc_count] = location.latitude
    print(location.address)
    print((location.latitude, location.longitude))


print('hello')
print('hai')

