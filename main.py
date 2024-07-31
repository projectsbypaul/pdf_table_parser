import camelot
import pandas as pd
import numpy as np
import os


def analyze_my_pdf(pdf_path: str):
    # this function gets count of pages in the pdf and return a camelot compotible str with trigger reading the last
    # 3 pages if pdf is longer than 3 pages
    # get list of pages from camelot.handlers.PDFHandler
    handler = camelot.handlers.PDFHandler(pdf_path)
    page_list = handler._get_pages(pages='all')

    n_pages = len(page_list)
    if n_pages <= 2:
        return 'all'
    else:
        int_seiten_zahlen = [n_pages - 1, n_pages]
        str_seiten_zahlen = ', '.join(str(zahl) for zahl in int_seiten_zahlen)
        return str_seiten_zahlen


def read_my_pdf(pdf_path: str, pages: str):
    # this function extracts tables from pdf and return pandas dataframes
    tables = camelot.read_pdf(pdf_path, flavor='stream', pages=pages)
    '''
    print(tables.n)
    print(tables[0])
    print(tables[0].parsing_report)
    '''
    table_df = []
    for i in range(len(tables)):
        table_df.append(tables[i].df)
    return table_df


def parse_dataframes(table_df: [pd.DataFrame], search_string: [str], path_name: str):
    df_of_search_strings = []

    for s_string in search_string:
        df_of_string = []

        for tdf in table_df:
            new_df = tdf[tdf.iloc[:, 0] == s_string]
            df_of_string.append(new_df)

        merged_df_of_string = pd.concat(df_of_string, axis=0)
        df_of_search_strings.append(merged_df_of_string)

    merged_df_of_search_strings = pd.concat(df_of_search_strings, axis=0)

    df_header = pd.DataFrame(np.nan, index=range(1), columns=range(merged_df_of_search_strings.shape[1]))
    df_header.iat[0, 0] = path_name[-20:]

    merged_df_of_search_strings = pd.concat([df_header, merged_df_of_search_strings], axis=0)

    return merged_df_of_search_strings


def parse_report(file_path, search_string):
    # get pages numbers
    seiten_zahlen = analyze_my_pdf(file_path)

    # get dataframes

    pdf_dataframes = read_my_pdf(file_path, seiten_zahlen)

    df_result = parse_dataframes(pdf_dataframes, searchstring, file_path)
    return df_result


def browse_dir_for_pdf(target_dir):
    pdf_names = [datei for datei in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, datei))]
    return pdf_names


def parse_reports(pdf_filenames, t_dir, search_string):
    df_results = []
    for pdf in pdf_filenames:
        print(f'Parsing report {pdf}')
        target_filename = os.path.join(t_dir, pdf)
        df_results.append(parse_report(target_filename, search_string))
        print(f'Parsing done')

    df_results_merged = pd.concat(df_results, axis=0)

    return df_results_merged


# main script
if __name__ == '__main__':
    target_dir = r'D:\KPI_project\Reports\test'

    jobname = 'CV_02_2024'

    searchstring = ['C032', 'C034', 'C040']

    pdf_names = browse_dir_for_pdf(target_dir)

    result = parse_reports(pdf_names, target_dir, searchstring)

    print()
