import camelot
import pandas as pd
import numpy as np
import os
import time
import configparser


def save_dataframe_to_csv(destination_dir, dataframe):
    return


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


def parse_reports(pdf_filenames, t_dir, search_string, header):
    df_results = []

    df_header = pd.DataFrame([header], columns=range(len(header)))

    # Ausgabe des DataFrames

    for pdf in pdf_filenames:
        print(f'Parsing report {pdf}')
        target_filename = os.path.join(t_dir, pdf)
        df_results.append(parse_report(target_filename, search_string))
        print(f'Parsing done')

    df_results_merged = pd.concat(df_results, axis=0)

    df_results_merged = pd.concat([df_header, df_results_merged], axis=0)

    return df_results_merged


# main script
if __name__ == '__main__':

    config = configparser.ConfigParser()

    config.read('config.ini')

    configuration = config['Setup']['Preset']

    save_dir = config[configuration]['save_dir']

    target = config[configuration]['target']

    dir_jobs = config[configuration]['dir_jobs']
    dir_jobs = [item.strip() for item in dir_jobs.split(',')]

    searchstring = config[configuration]['searchstring']
    searchstring = [item.strip() for item in searchstring.split(',')]

    header = config[configuration]['header']
    header = [item.strip() for item in header.split(',')]

    for dirname in dir_jobs:
        start = time.time()

        target_dir = os.path.join(target, dirname)

        jobname = f'{configuration}_{dirname}'

        print(f'Starting Job {jobname}')

        pdf_names = browse_dir_for_pdf(target_dir)

        result: pd.DataFrame = parse_reports(pdf_names, target_dir, searchstring, header)

        savefile = os.path.join(save_dir, jobname + '.xlsx')

        result.to_excel(savefile, sheet_name=jobname)

        end = time.time()

        print(f' Job done: {end - start}')
