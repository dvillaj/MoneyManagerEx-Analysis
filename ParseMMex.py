
from utils import *
import pandas as pd

df_data = pd.read_csv("data/M.csv") \
    .pipe(clean_data)

write_to_excel_levels(df_data.pipe(return_hogar_movements), 'Hogar')
write_to_excel_levels(df_data.pipe(return_despacho_movements), 'Despacho')
write_to_excel_beneficiarios(df_data, 'Beneficiarios')

plot_tree_map(df_data.pipe(return_hogar_movements), "Hogar") 
plot_tree_map(df_data.pipe(return_despacho_movements), "Despacho")

plot_tree_map(df_data.query("Fecha.dt.year == 2023").pipe(return_hogar_movements), "2023-Hogar") 
plot_tree_map(df_data.query("Fecha.dt.year == 2023").pipe(return_despacho_movements), "2023-Despacho")
