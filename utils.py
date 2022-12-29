import pandas as pd

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import locale
locale.setlocale(locale.LC_ALL, 'es_es')

def clean_data(df):
    return (df 
        .rename(columns = { 'Categoría' : 'Categoria' }) 
        .query("Tipo != 'Transfer'")            
        .assign(Fecha = lambda dataset: pd.to_datetime(dataset.Fecha, format = "%d/%m/%Y"),
                Subcategoria = lambda dataset : dataset.Categoria.str.split(':').str.get(1),
                Categoria = lambda dataset : dataset.Categoria.str.split(':').str.get(0),
                Transaccion = lambda df: df.Tipo.replace({'Deposit' : 'Abono', 'Withdrawal': 'Cargo'}),
                Tipo = lambda df: df.Categoria.map(lambda x: "Ingresos" if "Ingresos" in x else "Gastos"),
                Beneficiario = lambda df: df.Beneficiario.fillna("No Definido")
          )
        .query("Estado == 'R'")
        .filter(["Fecha", "Tipo", "Categoria", "Subcategoria", "Beneficiario", "Transaccion", "Importe"])
        .convert_dtypes()
      )



def return_despacho_movements(df):
        return ( df
            .query("Categoria.str.startswith('Despacho')") 
            .assign(Categoria = lambda df: df.Categoria.str[11:]) 
        )


def return_hogar_movements(df):
        return (df 
          .assign(
            Beneficiario = lambda df: df.apply(lambda dataset: "No Definido" if dataset.Categoria.startswith("Despacho") else dataset.Beneficiario, axis = 'columns'), 
            Subcategoria = lambda df: df.apply(lambda dataset: "Despacho" if dataset.Categoria.startswith("Despacho") else dataset.Subcategoria, axis = 'columns'), 
            Tipo = lambda df: df.apply(lambda dataset: "Ingresos" if dataset.Categoria.startswith("Despacho") else dataset.Tipo, axis = 'columns'),
            Categoria = lambda df: df.Categoria.map(lambda x: "Ingresos" if x.startswith("Despacho") else x)
          )
        )

def return_beneficiarios(df):
    return (df 
        .assign(Categoria = lambda df: df.Categoria + ":" + df.Subcategoria) 
        .filter(["Beneficiario", "Categoria"]) 
        .drop_duplicates() 
        .dropna() 
        .assign(Duplicado = lambda df: df.groupby(["Beneficiario"]).Categoria.transform("count").map(lambda x: "Si" if x > 1 else "No"))
        .sort_values(["Beneficiario"])
    )
    
def return_treemap_data(df):
    import datetime

    return (df
        .assign(MaxDate = lambda df: pd.to_datetime((df.Fecha.max() - datetime.timedelta(days=0)).date()),
                MinDate = lambda df: pd.to_datetime((df.Fecha.min() - datetime.timedelta(days=0)).date()))
        .query("Fecha.between(MinDate, MaxDate)")
        .pipe(pivot_by_category, ["Tipo", "Categoria", "Subcategoria", "Beneficiario"])
        # .filter(["Tipo", "Categoria", "Subcategoria", "Beneficiario", "Media", "Total"])
        .query("not (Tipo == 'Gastos' and Total > 0)")
        .assign(Media = lambda df: df.Media.abs(),
                Total = lambda df: df.Total.abs()
               )
     )


def total_rows(df, columns):
    if len(columns) > 1:
        group_by_columns = columns[:-1]
        first_column = group_by_columns[-1]
        last_column = columns[-1]

        df_totales = df \
            .groupby(group_by_columns).sum().reset_index() \
            .query(first_column + " not in ['Total']")

        df_totales[last_column] = df_totales[first_column].map(lambda x: "Total" if x == "Total" else "Total " + x)
    else:
        last_column = columns[0]
        df_totales = df.sum(numeric_only=True).to_frame().transpose()
        df_totales[last_column] = "Total"

    return df_totales


def pivot_table(df, left_columns, top_columns, value_column,  aggfunc_name="sum"):
    return (df
            .pivot_table(index =left_columns,
                       columns = top_columns,
                       values=value_column,
                       aggfunc=aggfunc_name,
                       fill_value = 0)
            .reset_index()
            .rename_axis(columns = None)
 )


def sort_columns(df, columns):
    cols = columns + sorted(df.columns[len(columns):].to_list(), reverse=True)
    return df.filter(cols)


def pivot_by_category(df, category):
    fist_item = category[0]

    return (
        df
         .assign(Mes = lambda dataset: dataset.Fecha.dt.to_period('M').dt.to_timestamp())
         .pipe(pivot_table, left_columns = category, top_columns = "Mes", value_column = "Importe")
         .pipe(sort_columns, category)
         .rename(columns = lambda col: col if isinstance(col, str) else col.strftime("%B %Y").title())
         .assign(
            Media = lambda dataset: dataset.mean(axis = 'columns'),
            Total = lambda dataset: dataset.drop(columns = "Media").sum(axis = 'columns')
         )
        .reset_index(drop = True)
        .fillna({ fist_item : "Total" })
        .rename_axis(None, axis='columns')
        .round(2)
        .convert_dtypes()
    )


def pivot_by_category_totals(df, columns):
    df_pivot = df.pipe(pivot_by_category, columns)
    df_all_totales = df_pivot
    for n in range(0,len(columns)):
        df_all_totales = pd.concat([df_all_totales, df_pivot.pipe(total_rows, columns[:n + 1])])

    for n in range(0,len(columns)):
        df_all_values = df_all_totales[columns[n]].value_counts().index.to_list()
        df_all_values_total = list(filter(lambda x: x.startswith("Total"), df_all_values))
        df_all_values = list(filter(lambda x: not x.startswith("Total"), df_all_values))
        df_all_values = sorted(df_all_values)
        df_all_values.extend(sorted(df_all_values_total))

        df_all_totales[columns[n]] = pd.Categorical(
            df_all_totales[columns[n]],
            categories=df_all_values,
            ordered=True)

    return (df_all_totales
                .sort_values(columns)
                .reset_index(drop = True)
            )

    
def highlight_total(df, column_name):
    if str(df[column_name]).startswith('Total') :
        return ['font-weight: bold' for v in df]
    else:
        return ['background-color: white' for v in df]


def highlight_important_columns(df):
    return ["background-color: #FFF4D2" for v in df]


def highlight_current_month(df):
    return ["color: blue" for v in df]


def style_locale_es(df):
    return df.style.format(decimal = ',', thousands=".", precision = 2)


def get_current_month():
    from datetime import datetime  
    return datetime.today().strftime("%B %Y").title()

         
def style_dataframe_totals(df, columns):
    df_styled = df.pipe(style_locale_es)

    for n in range(0,len(columns)):
        df_styled = df_styled.apply(highlight_total, axis = 1, column_name = columns[n])

    df_styled = df_styled.apply(highlight_important_columns, subset=columns, axis=1)
    # df_styled = df_styled.apply(highlight_current_month, subset=[get_current_month()], axis=1)
        
    return df_styled

def get_col_widths(dataframe):
    sizes = []
    for column_name in dataframe.columns.to_list():
        max_value = max([max(dataframe[column_name].str.len()), len(column_name) + 2])
        sizes.extend([max_value])

    return sizes


def get_col_widths_months(dataframe, columns):
    sizes = []
    for index, column_name in enumerate(dataframe.columns.to_list()):

        if (index < len(columns)):
            max_value = max([dataframe[column_name].str.len().max(), len(column_name)])
        else:
            max_value = max([dataframe[column_name].map(lambda value: len(f'{value:,.2f}')).max() + 1, len(column_name)]) + 1

        sizes.extend([max_value])

    return sizes


def excel_header_color(xlsx, worksheet, df, columns = None):
    # Add a header format.
    header_format = xlsx.book.add_format({
        'bold': True,
        'text_wrap': False,
        'valign': 'top',
        'align': 'left',
        'fg_color': '#FFE8A4',
        'border': 1})

    header_format_values = xlsx.book.add_format({
        'bold': True,
        'text_wrap': False,
        'valign': 'top',
        'align': 'center',
        'fg_color': '#FFE8A4',
        'border': 1})


    # Write the column headers with the defined format.
    for col_num, value in enumerate(df.columns.values):
        if (columns and col_num >= len(columns)):
            worksheet.write(0, col_num, value, header_format_values)
        else:
            worksheet.write(0, col_num, value, header_format)


def excel_autofilter(worksheet, df, columns):
    worksheet.autofilter('A1:' + chr(ord('@')+len(columns)) + str(df.shape[0] + 1))


def excel_border(xlsx, worksheet, df):
    border_format= xlsx.book.add_format({
                            'border': 1
                           })
    worksheet.conditional_format( 'A1:' + chr(ord('@')+ df.shape[1]) + str(df.shape[0] + 1) , 
                                 { 'type' : 'no_errors',
                                  'format' : border_format} )

def excel_columns_size(worksheet, columns_size):
    for i, width in enumerate(columns_size):
        worksheet.set_column(i, i, width)


def excel_format_main_columns(xlsx, worksheet, df, columns):
    formatter = xlsx.book.add_format({
            'bold': True,
            'text_wrap': False,
            'fg_color': '#FFF4D2'})

    for i,column in enumerate(columns):
        range=f"{chr(ord('A') + i)}2"
        values = map(
            lambda x: None if x == 'nan' else x,
            df[column].astype('str').to_list()
        )
        worksheet.write_column(range, values, formatter)


def excel_format_size_currency(xlsx, worksheet, columns_lengths, columns):
    currency_formater = xlsx.book.add_format({'num_format': '#,##0.00 €;[RED] -#,##0.00 €'})

    for i, width in enumerate(columns_lengths):
        if i < len(columns):
            worksheet.set_column(i, i, width)
        else:
            worksheet.set_column(i, i, width, currency_formater)


def save_to_excel_pivot(xlsx, df, columns, sheet_name):    
    workbook = xlsx.book        
    df_pivot = df.pipe(pivot_by_category_totals, columns)

    (df_pivot 
        .to_excel(xlsx, sheet_name, index = False)
     )

    worksheet = workbook.get_worksheet_by_name(sheet_name)

    columns_lengths = get_col_widths_months(df_pivot, columns)
    
    excel_format_size_currency(xlsx, worksheet, columns_lengths, columns)
    excel_format_main_columns(xlsx, worksheet, df_pivot, columns)

    excel_header_color(xlsx, worksheet, df_pivot, columns)
    excel_autofilter(worksheet, df_pivot, columns)
    excel_border(xlsx, worksheet, df_pivot)
    

def save_to_excel(xlsx, df, sheet_name):
    workbook = xlsx.book

    df.to_excel(xlsx, sheet_name, index = False)
    worksheet = workbook.get_worksheet_by_name(sheet_name)

    excel_columns_size(worksheet, get_col_widths(df))
    excel_header_color(xlsx, worksheet, df)
    excel_autofilter(worksheet, df, df.columns.to_list())
    excel_border(xlsx, worksheet, df)
    

def write_to_excel_levels(df, excel_name, target_dir = "./target"):
    xlsx = pd.ExcelWriter(f"{target_dir}/{excel_name}.xlsx", engine='xlsxwriter')

    save_to_excel_pivot(xlsx, df, ["Tipo"], "Nivel 1")
    save_to_excel_pivot(xlsx, df, ["Tipo", "Categoria"], "Nivel 2")
    save_to_excel_pivot(xlsx, df, ["Tipo", "Categoria", "Subcategoria"], "Nivel 3")
    save_to_excel_pivot(xlsx, df, ["Tipo", "Categoria", "Subcategoria", "Beneficiario"], "Nivel 4")
    if (excel_name == 'Despacho'):
        save_to_excel_pivot(xlsx, df.query("Categoria == 'Ingresos'"), ["Beneficiario", "Subcategoria", "Transaccion"], "Beneficiarios")

    xlsx.close()


def write_to_excel_beneficiarios(df, excel_name, target_dir = "./target"):
    xlsx = pd.ExcelWriter(f"{target_dir}/{excel_name}.xlsx", engine='xlsxwriter')

    save_to_excel(xlsx, df.pipe(return_hogar_movements).pipe(return_beneficiarios), "Hogar")
    save_to_excel(xlsx, df.pipe(return_despacho_movements).pipe(return_beneficiarios), "Despacho")

    xlsx.close()


def plot_tree_map(df, tipo, target_dir = "./target"):
    import plotly.express as px
    import plotly

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