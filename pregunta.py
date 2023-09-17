"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():
    #
    # Inserte su código aquí
    #

    header_df = pd.read_fwf("clusters_report.txt", nrows=2, header=None, colspecs=([0, 7], [9, 23], [25, 39], [41, 67]))
    header = [header_df.iloc[0, 0], " ".join(header_df.iloc[:, 1]), " ".join(header_df.iloc[:, 2]),
              header_df.iloc[0, 3]]
    # df = pd.read_csv("clusters_report.txt", sep="\t", skiprows=4, header=None,delim_whitespace=True)
    # df = df.iloc[:, 0].apply(lambda x: x.replace("   ", " ").replace("  ", " ").replace("   ", "  "))
    # df = df.str.split("\D\D", expand=True, n=3)

    df = pd.read_fwf("clusters_report.txt", skiprows=4, header=None)
    df = df.ffill()
    df[3] = df.groupby([0, 1, 2]).transform(lambda x: ' '.join(x))
    out = df.drop_duplicates()
    out.columns = header

    ## Step 2 clean data

    out.columns = [x.replace(" ", "_").lower() for x in out.columns]

    out2 = out.copy()
    out2.loc[:, out.columns[2]] = out.loc[:, out.columns[2]].apply(lambda x: float(x.split(" %")[0].replace(",", ".")))

    out3 = out2.copy()
    out3.loc[:, out.columns[3]] = out.loc[:, out.columns[3]].apply(
        lambda x: x.replace(",", ", ").replace("   ", " ").replace("  ", " ").replace(".", "").replace("  ", " "))
    return out3


