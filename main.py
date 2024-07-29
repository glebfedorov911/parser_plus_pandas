import pandas as pd

# df = pd.read_excel("file.xlsx")

# df = df.apply(lambda col: col.astype(object))

# data_as_list = df.values.tolist()

# data_as_dict = df.to_dict(orient="list")

# for i in data_as_list:
#     print(i)

# df_to_list = df.values.tolist()
def create(df_to_list):
    brands = []
    nums = []
    for i in df_to_list:
        brands.append((df_to_list.index(i), i[2]))
        nums.append((df_to_list.index(i), i[3]))
    
    return brands, nums

# df.at[0, 'Лого'] = 'Лого'

# df.to_excel("file(1).xlsx")