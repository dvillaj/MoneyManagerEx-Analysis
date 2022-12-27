
from utils import *
import pandas as pd

df_data = pd.read_csv("data/M.csv") \
    .pipe(clean_data)

write_to_excel_levels(df_data.pipe(return_hogar_movements), 'Hogar')
write_to_excel_levels(df_data.pipe(return_despacho_movements), 'Despacho')
write_to_excel_beneficiarios(df_data, 'Beneficiarios')

plot_tree_map(df_data.pipe(return_hogar_movements), "Hogar") 
plot_tree_map(df_data.pipe(return_despacho_movements), "Despacho")
