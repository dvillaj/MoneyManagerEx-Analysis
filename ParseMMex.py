import plotly.express as px
from utils import *
import pandas as pd
import plotly
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def plot_tree_map(df, Tipo):
    # color = "Categoria",
    # px.Constant('All'),
    # color_continuous_scale = 'RdBu',
    fig = px.treemap(df,
                path=[ 'Tipo', 'Categoria', 'Subcategoria', 'Beneficiario'],
                values = "AbsoluteImporte",
                title= f"{Tipo}: Gastos / Ingresos por Categoria"
    )

    fig.update_layout(
            title_font_size=42,
            title_font_family="Arial"
        )


    plotly.offline.plot(fig, filename = f'target/TreeMap_{Tipo}.html')
    

df_data = pd.read_csv("data/M.csv") \
    .pipe(clean_data)

write_to_excel(df_data.pipe(return_despacho_movements), 'target/Hogar.xlsx')
write_to_excel(df_data.pipe(return_hogar_movements), 'target/Despacho.xlsx')

plot_tree_map(treemap_data(df_data, return_hogar_movements), "Hogar")
plot_tree_map(treemap_data(df_data, return_despacho_movements), "Despacho")
