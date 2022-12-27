import plotly.express as px
from utils import *
import pandas as pd
import plotly
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def plot_tree_map(df, tipo, target_dir = "target"):
    # color = "Categoria",
    # px.Constant('All'),
    # color_continuous_scale = 'RdBu',
    fig = px.treemap(df.pipe(return_treemap_data),
                path=[ 'Tipo', 'Categoria', 'Subcategoria', 'Beneficiario'],
                values = "Total",
                title= f"{tipo}: Gastos / Ingresos totales por Categoria"
    )

    fig.update_layout(
            title_font_size=42,
            title_font_family="Arial"
        )


    plotly.offline.plot(fig, filename = f'{target_dir}/TreeMap_{tipo}.html')
    

df_data = pd.read_csv("data/M.csv") \
    .pipe(clean_data)

write_to_excel(df_data.pipe(return_hogar_movements), 'target/Hogar.xlsx')
write_to_excel(df_data.pipe(return_despacho_movements), 'target/Despacho.xlsx')

plot_tree_map(df_data.pipe(return_hogar_movements), "Hogar")
plot_tree_map(df_data.pipe(return_despacho_movements), "Despacho")
