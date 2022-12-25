import pandas as pd
import numpy as np
import datetime


def clean_data(df):
    today = datetime.date.today()
    year = today.year
    month = today.month
    
    return df \
        .rename(columns = { 'Categoría' : 'Categoria' }) \
        .assign(Fecha = lambda dataset : pd.to_datetime(dataset.Fecha, format = "%d/%m/%Y"),
              Subcategoria = lambda dataset : dataset.Categoria.str.split(':').str.get(1),
              Categoria = lambda dataset : dataset.Categoria.str.split(':').str.get(0),
              Date = lambda dataset: dataset.Fecha,
              Year = lambda dataset: dataset.Fecha.dt.year,
              Month = lambda dataset: dataset.Fecha.dt.month,
              Tipo = lambda df: df.Categoria.map(lambda x: "Ingresos" if "Ingresos" in x else "Gastos"),
              Beneficiario = lambda df: df.Beneficiario.fillna("No Definido")   
         ) \
        .query("Month <= @month and Year == @year") \
        .query("Categoria not in ['Transferencia', 'Préstamo']") \
        .query("Cuenta not in ['DEUDAS']") \
        .filter(["Identificador", "Date", "Year", "Month", "Cuenta", "Tipo", "Categoria", "Subcategoria", "Beneficiario", "Importe", "Divisa", "Número", "Estado", "Notas" ]) 


def return_despacho_movements(df):
        return df \
            .query("Categoria.str.startswith('Despacho')") \
            .assign(Categoria = lambda df: df.Categoria.str[11:]) 


def return_hogar_movements(df):
        return df \
          .assign(
            Beneficiario = lambda df: df.apply(lambda dataset: "No Definido" if dataset.Categoria.startswith("Despacho") else dataset.Beneficiario, axis = 'columns'), 
            Subcategoria = lambda df: df.apply(lambda dataset: "Despacho" if dataset.Categoria.startswith("Despacho") else dataset.Subcategoria, axis = 'columns'), 
            Tipo = lambda df: df.apply(lambda dataset: "Ingresos" if dataset.Categoria.startswith("Despacho") else dataset.Tipo, axis = 'columns'),
            Categoria = lambda df: df.Categoria.map(lambda x: "Ingresos" if x.startswith("Despacho") else x)
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

    return df_all_totales.sort_values(columns) \
                    .reset_index(drop = True)


def pivot_by_category(df, category):
    today = datetime.date.today()
    year = today.year
    month = today.month
    
    group_list = ["Year", "Month"]
    group_list.extend(category)
    fist_item = category[0]
    return df \
      .groupby(group_list, as_index = False).Importe.sum() \
      .drop(columns = ["Year"]) \
      .pivot_table(index = category, columns = "Month", values = "Importe", aggfunc = "sum") \
      .reset_index() \
      .fillna(0) \
      .assign(Media = lambda df: avg_year(df, month),
              Total = lambda df: total_year(df, month)) \
      .rename(columns = get_months()) \
      .reset_index(drop = True) \
      .fillna({ fist_item : "Total" }) \
      .rename_axis(None, axis=1)


def total_year(df, month):
    total = 0
    for n in range(month):
        total = total + df[n + 1]

    return round(total,2)


def avg_year(df, month):
    total = 0
    for n in range(month):
        total = total + df[n + 1]

    return round(total / month,2)


def get_months():
    return {
        1 : 'Enero',
        2 : 'Febrero',
        3 : 'Marzo',
        4 : 'Abril',
        5 : 'Mayo',
        6 : 'Junio',
        7 : 'Julio',
        8 : 'Agosto',
        9 : 'Septiembre',
        10 : 'Octubre',
        11 : 'Noviembre',
        12 : 'Diciembre'
      }


def get_month_name(month):
    return get_months()[month]



def varnames_as_values(df):
     return df \
        .set_axis(df.iloc[0].to_list(),
              axis = 'columns') \
        .drop(0)


def replace_first_column(df, colname):
    columns = df.columns.to_list()
    columns[0] = colname
    return df.set_axis(columns,
                   axis = 'columns')


def drop_last_column(df):
     return df.drop(columns=df.columns[-1])
    
    
def add_serie(chart, sheet_name, column, max_row, color_name, serie_name = None ):
    
    if not serie_name:
        serie_name = f'={sheet_name}!${column}$1'
    
    chart.add_series({
        #'name':       f'={sheet_name}!${column}$1',
        'name':       serie_name,
        'categories': f'={sheet_name}!$A$2:$A${max_row}',
        'values':     f'={sheet_name}!${column}$2:${column}${max_row}',
        'line' :     { 'color' :  f'{color_name}' }
    })


def filter_total(df, columns):
    df_total = df.tail(1)
    df_data = df[:-1] \
          .assign(Total = lambda df: df.Total.abs()) 
    columns.extend(['Total'])
    return pd.concat([df_data, df_total]) \
        .filter(columns) 


def acum_total(df):
    return df \
        .transpose() \
        .reset_index(drop = False) \
        .pipe(varnames_as_values) \
        .pipe(drop_last_column) \
        .pipe(replace_first_column, "Mes") \
        .query("Mes not in ('Total', 'Media')") \
        .assign(GastosAcum = lambda df: df.Gastos.cumsum(),
                IngresosAcum = lambda df: df.Ingresos.cumsum(),
                Total = lambda df: df.Gastos + df.Ingresos,
                TotalAcum = lambda df: df.GastosAcum + df.IngresosAcum) \
        .assign(Gastos = lambda df: df.Gastos.abs(),
                GastosAcum = lambda df: df.GastosAcum.abs()) \
        .reset_index(drop = True) \
        .filter(["Mes", "Gastos", "Ingresos", "Total", "GastosAcum", "IngresosAcum", "TotalAcum"])
    
    
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


def style_dataframe_totals(df, columns):
    today = datetime.date.today()
    month = today.month
    
    df_styled = df \
      .style.format(thousands=",", precision = 2)

    for n in range(0,len(columns)):
        df_styled = df_styled.apply(highlight_total, axis = 1, column_name = columns[n])

    df_styled = df_styled.apply(highlight_important_columns, subset=columns, axis=1)
    # df_styled = df_styled.apply(highlight_current_month, subset=[ get_month_name(month) ], axis=1)
        
    return df_styled


def reoder_columns(df, columns):
    today = datetime.date.today()
    month = today.month
    
    df_columns = df.columns.to_list()
    keep_columns = columns + [ get_month_name(month), "Media" ]

    for month in keep_columns:
        df_columns.remove(month)
        
    return df.filter(keep_columns + df_columns)


def get_col_widths(dataframe):
    sizes = []
    for column in dataframe.columns.to_list():
        max_value = max([max(dataframe[column].str.len()), len(column)])
        sizes.extend([max_value])

    return sizes


def get_col_widths_months(dataframe, columns):
    sizes = []
    for column in columns:
        max_value = max([max(dataframe[column].str.len()), len(column)])
        sizes.extend([max_value])

    return sizes + [10 for col in range(0, len(dataframe.columns) - len(columns))]


def excel_header_color(xlsx, worksheet, df):
    # Add a header format.
    header_format = xlsx.book.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#FFE8A4',
        'border': 1})

    # Write the column headers with the defined format.
    for col_num, value in enumerate(df.columns.values):
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


def return_beneficiarios(df):
    return df \
        .assign(Categoria = lambda df: df.Categoria + ":" + df.Subcategoria) \
        .filter(["Beneficiario", "Categoria"]) \
        .drop_duplicates() \
        .dropna() \
        .assign(Duplicado = lambda df: df.groupby("Beneficiario").Categoria.transform("count").map(lambda x: "Si" if x > 1 else "No")) \
        .sort_values("Beneficiario")
    
        
def save_to_excel_pivot(xlsx, df, columns, sheet_name_arg = None):
    if not sheet_name_arg:
        sheet_name = columns[-1]
    else:
        sheet_name = sheet_name_arg
    
    workbook = xlsx.book        
    df_pivot = df.pipe(pivot_by_category_totals, columns) \
          .pipe(reoder_columns, columns)

    df_pivot \
        .pipe(style_dataframe_totals, columns) \
        .to_excel(xlsx, sheet_name, index = False)
    worksheet = workbook.get_worksheet_by_name(sheet_name)

    excel_columns_size(worksheet, get_col_widths_months(df_pivot, columns))
    excel_header_color(xlsx, worksheet, df_pivot)
    excel_autofilter(worksheet, df_pivot, columns)
    excel_border(xlsx, worksheet, df_pivot)
    

def save_to_excel(xlsx, df, sheet_name_arg = None):
    if not sheet_name_arg:
        sheet_name = columns[-1]
    else:
        sheet_name = sheet_name_arg

    workbook = xlsx.book

    df.to_excel(xlsx, sheet_name, index = False)
    worksheet = workbook.get_worksheet_by_name(sheet_name)

    excel_columns_size(worksheet, get_col_widths(df))
    excel_header_color(xlsx, worksheet, df)
    excel_autofilter(worksheet, df, df.columns.to_list())
    excel_border(xlsx, worksheet, df)
    

def write_to_excel(df, excel_name):
    xlsx = pd.ExcelWriter(excel_name, engine='xlsxwriter')

    save_to_excel_pivot(xlsx, df, ["Tipo"], "Nivel 1")
    save_to_excel_pivot(xlsx, df, ["Tipo", "Categoria"], "Nivel 2")
    save_to_excel_pivot(xlsx, df, ["Tipo", "Categoria", "Subcategoria"], "Nivel 3")
    save_to_excel_pivot(xlsx, df, ["Tipo", "Categoria", "Subcategoria", "Beneficiario"], "Nivel 4")
    save_to_excel_pivot(xlsx, df, ["Beneficiario"], "Beneficiarios")
    save_to_excel(xlsx, df.pipe(return_beneficiarios), "Beneficario - Categoría")

    xlsx.close()


def group_data(df):
    category = ["Tipo", "Year", "Month", "Categoria", "Subcategoria", "Beneficiario"]
    return df \
      .groupby(category, as_index = False).Importe.sum()    


def write_raw_to_excel(df, excel_name):
    xlsx = pd.ExcelWriter(excel_name, engine='xlsxwriter')
    df.pipe(group_data).to_excel(xlsx, "Data", index = False)
    xlsx.close()


def treemap_data(df, movement_function):
    return df  \
        .pipe(movement_function) \
        .assign(MaxDate = lambda df: pd.to_datetime((df.Date.max() - datetime.timedelta(days=0)).date()),
                MinDate = lambda df: pd.to_datetime((df.Date.min() - datetime.timedelta(days=0)).date())) \
        .query("Date.between(MinDate, MaxDate)") \
        .groupby(["Tipo", "Categoria", "Subcategoria", "Beneficiario"], as_index = False).Importe.sum() \
        .assign(AbsoluteImporte = lambda df: df.Importe.abs()) \
        .query("not (Tipo == 'Gastos' and Importe > 0)")