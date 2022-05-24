import pandas as pd

file_name = "BW_2050"

load_data_year_1 = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-H2-G/BW_2040.xlsx",
                                 index_col=0)

load_data_year_2 = pd.read_excel("C:/Users/work/pypsa_thesis/data/hydrogen/TN-H2-G/BW_2050_original.xlsx",
                                 index_col=0)

dataframe_year_1 = pd.DataFrame(load_data_year_1)
dataframe_year_2 = pd.DataFrame(load_data_year_2)
dataframe_year_1.reset_index()
dataframe_year_2.reset_index()

location_list_year_2 = []
demand_value_year_2 = []

for row_count_x in range(len(dataframe_year_2.index)):
    for row_count_y in list(dataframe_year_1.index):
        if dataframe_year_2.index[row_count_x] == row_count_y:
            location_list_year_2.append(dataframe_year_2.index[row_count_x])
            demand_value_year_2.append(dataframe_year_2['Total valueInG'][row_count_x])

df_year_2_to_excel = pd.DataFrame(index=location_list_year_2,
                                  columns=['demand_value', 'x', 'y'])

df_year_2_to_excel['demand_value'] = demand_value_year_2

df_year_2_to_excel.to_excel("{}.xlsx".format(file_name))

