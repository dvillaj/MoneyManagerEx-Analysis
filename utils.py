import pandas as pd
import numpy as np
import datetime

def clean_data(df):
    return (df 
        .rename(columns = { 'Categoría' : 'Categoria' }) 
        .query("Tipo != 'Tranfer'")            
        .assign(Fecha = lambda dataset: pd.to_datetime(dataset.Fecha, format = "%d/%m/%Y"),
                Subcategoria = lambda dataset : dataset.Categoria.str.split(':').str.get(1),
                Categoria = lambda dataset : dataset.Categoria.str.split(':').str.get(0),
                Tipo = lambda df: df.Categoria.map(lambda x: "Ingresos" if "Ingresos" in x else "Gastos"),
                Beneficiario = lambda df: df.Beneficiario.fillna("No Definido")
          )
        .query("Estado == 'R'")
        .filter(["Fecha", "Tipo", "Categoria", "Subcategoria", "Beneficiario", "Importe"])
      )



def return_despacho_movements(df):
        return ( df
            .query("Categoria.str.startswith('Despacho')") 
            .assign(Categoria = lambda df: df.Categoria.str[11:]) 
            .convert_dtypes()
        )


def return_hogar_movements(df):
        return (df 
          .assign(
            Beneficiario = lambda df: df.apply(lambda dataset: "No Definido" if dataset.Categoria.startswith("Despacho") else dataset.Beneficiario, axis = 'columns'), 
            Subcategoria = lambda df: df.apply(lambda dataset: "Despacho" if dataset.Categoria.startswith("Despacho") else dataset.Subcategoria, axis = 'columns'), 
            Tipo = lambda df: df.apply(lambda dataset: "Ingresos" if dataset.Categoria.startswith("Despacho") else dataset.Tipo, axis = 'columns'),
            Categoria = lambda df: df.Categoria.map(lambda x: "Ingresos" if x.startswith("Despacho") else x)
          )
            .convert_dtypes()
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
    from datetime import datetime
    import locale

    locale.setlocale(locale.LC_ALL, 'es_es')
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

    return df_all_totales.sort_values(columns) \
                    .reset_index(drop = True)

    
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
    return (df 
        .assign(Categoria = lambda df: df.Categoria + ":" + df.Subcategoria) 
        .filter(["Beneficiario", "Categoria"]) 
        .drop_duplicates() 
        .dropna() 
        .assign(Duplicado = lambda df: df.groupby(["Beneficiario"]).Categoria.transform("count").map(lambda x: "Si" if x > 1 else "No"))
        .sort_values(["Beneficiario"])
    )
    
        
def save_to_excel_pivot(xlsx, df, columns, sheet_name_arg = None):
    if not sheet_name_arg:
        sheet_name = columns[-1]
    else:
        sheet_name = sheet_name_arg
    
    workbook = xlsx.book        
    df_pivot = df.pipe(pivot_by_category_totals, columns)

    df_pivot \
        .pipe(style_dataframe_totals, columns) \
        .to_excel(xlsx, sheet_name, index = False)
    worksheet = workbook.get_worksheet_by_name(sheet_name)

    excel_columns_size(worksheet, get_col_widths_months(df_pivot, columns))
    excel_header_color(xlsx, worksheet, df_pivot)
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
    

def write_to_excel(df, excel_name):
    xlsx = pd.ExcelWriter(excel_name, engine='xlsxwriter')

    save_to_excel_pivot(xlsx, df, ["Tipo"], "Nivel 1")
    save_to_excel_pivot(xlsx, df, ["Tipo", "Categoria"], "Nivel 2")
    save_to_excel_pivot(xlsx, df, ["Tipo", "Categoria", "Subcategoria"], "Nivel 3")
    save_to_excel_pivot(xlsx, df, ["Tipo", "Categoria", "Subcategoria", "Beneficiario"], "Nivel 4")
    save_to_excel_pivot(xlsx, df, ["Beneficiario"], "Beneficiarios")
    save_to_excel(xlsx, df.pipe(return_beneficiarios), "Beneficario - Categoría")

    xlsx.close()


def treemap_data(df):
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