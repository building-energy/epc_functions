# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 14:42:31 2023

@author: cvskf
"""

import sqlite3
import os
import json
import importlib.resources as pkg_resources
import epc_functions
import subprocess
import urllib.request
import csvw_functions
import csvw_functions_extra
import zipfile
import datetime
import csv

_default_data_folder='_data'  # the default
_default_database_name='epc_data.sqlite'
_default_fp_zip=os.path.join(_default_data_folder,'all-domestic-certificates.zip')

urllib.request.urlcleanup()



#%% data folder

def get_csv_zip_extract_paths_in_zip(
        zip_filename = 'all-domestic-certificates.zip',
        data_folder = '_data'
        ):
    ""
    fp_zip = \
        os.path.join(
            data_folder,
            zip_filename)
    
    z = zipfile.ZipFile(fp_zip)
        
    zip_extract_paths = z.namelist()
    
    csv_zip_extract_paths = [x for x in zip_extract_paths if os.path.splitext(x)[1]=='.csv']
    
    return csv_zip_extract_paths


def extract_and_import_data(
        zip_filename = 'all-domestic-certificates.zip',
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        csv_zip_extract_paths = None,
        set_certificates = True,
        set_recommendations = True,
        inspection_date_start=None,
        inspection_date_end=None,
        verbose=False
        ):
    ""
    
    csv_file_names = \
        _extract_table_group(
            zip_filename = zip_filename,
            data_folder = data_folder,
            csv_zip_extract_paths = csv_zip_extract_paths,
            set_certificates = set_certificates,
            set_recommendations = set_recommendations,
            inspection_date_start = inspection_date_start,
            inspection_date_end = inspection_date_end,
            verbose = verbose
            )
    
    _import_table_group_to_sqlite(
            data_folder = data_folder,
            database_name = database_name,
            csv_file_names = csv_file_names, 
            verbose = verbose
            )
    
    
    
def _extract_table_group(
        zip_filename = 'all-domestic-certificates.zip',
        data_folder = '_data',
        csv_zip_extract_paths = None,
        set_certificates = True,
        set_recommendations = True,
        inspection_date_start=None,
        inspection_date_end=None,
        verbose = False
        ):
    """
    """
    # saves metadata_table_group file
    metadata_document_location = \
        _create_metadata_table_group_file_pre_file_extraction(
            zip_filename = zip_filename,
            data_folder = data_folder,
            csv_zip_extract_paths = csv_zip_extract_paths,
            set_certificates = set_certificates,
            set_recommendations = set_recommendations
            )
        
    # extract files
    csvw_functions_extra.download_table_group(
        metadata_document_location = metadata_document_location,
        data_folder = data_folder,
        overwrite_existing_files = True,
        verbose = verbose
        )
    
    metadata_table_group_dict = \
        csvw_functions_extra.get_metadata_table_group_dict(
            data_folder = data_folder,
            metadata_filename = 'epc_tables-metadata.json'
            )
        
    csv_file_names = \
        [table['https://purl.org/berg/csvw_functions_extra/vocab/csv_file_name']['@value']
         for table in metadata_table_group_dict['tables']]
        
    # filter csv file
    if not inspection_date_start is None or not inspection_date_end is None:
        
        for csv_file_name in csv_file_names:
            
            fp_csv = \
                os.path.join(
                    data_folder,
                    csv_file_name
                    )
            
            if verbose:
                print(f'--- filtering {csv_file_name} ---')
            
            if csv_file_name.endswith('certificates.csv'):
        
                _filter_csv_file(
                        fp=fp_csv,
                        fp_out=fp_csv,
                        start_date=inspection_date_start,
                        end_date=inspection_date_end,
                        var='INSPECTION_DATE'
                        )

    return csv_file_names


def _create_metadata_table_group_file_pre_file_extraction(
        zip_filename = 'all-domestic-certificates.zip',
        data_folder = '_data',
        csv_zip_extract_paths = None,
        set_certificates = True,
        set_recommendations = True
        ):
    ""
    # # get zip filepath
    # fp_zip = \
    #     os.path.join(
    #         data_folder,
    #         zip_filename)
    
    # get csv_zip_extract_paths
    csv_zip_extract_paths = \
        csvw_functions_extra.convert_to_iterator(
            csv_zip_extract_paths
            )
    
    if len(csv_zip_extract_paths) == 0:
        csv_zip_extract_paths = \
            get_csv_zip_extract_paths_in_zip(
                    zip_filename
                    )
            
    # get metadata table group dict
    url = 'https://raw.githubusercontent.com/building-energy/epc_functions/main/epc_tables-metadata.json'
    request = urllib.request.urlopen(url)
    metadata_table_group_dict = json.loads(
        request.read().decode()
        )
    
    
    metadata_table_group_dict = {
        "$schema":"https://raw.githubusercontent.com/stevenkfirth/csvw_metadata_json_schema/main/schema_files/table_group_description.schema.json",
        "@context": "http://www.w3.org/ns/csvw",
        "@type": "TableGroup",
        "tables": []
        }
    
    metadata_schema_dict_certificates = \
        csvw_functions.validate_schema_metadata(
                'https://raw.githubusercontent.com/building-energy/epc_functions/main/epc_domestic_certificates-schema-metadata.json'
                )
    metadata_schema_dict_certificates.pop('@context')
    #print(metadata_schema_dict_certificates)
    
    metadata_schema_dict_recommendations = \
        csvw_functions.validate_schema_metadata(
                'https://raw.githubusercontent.com/building-energy/epc_functions/main/epc_domestic_recommendations-schema-metadata.json'
                )
    metadata_schema_dict_recommendations.pop('@context')
    #print(metadata_schema_dict_recommendations)
    
    for csv_zip_extract_path in csv_zip_extract_paths:
        
        basename=os.path.basename(csv_zip_extract_path)
        #print('basename', basename)
        dirname=os.path.dirname(csv_zip_extract_path)
        #print('dirname', dirname)
        
        if not basename in ['certificates.csv','recommendations.csv']:
            continue
        
        csv_file_name=f'{dirname}_{basename}'
        
        #zip_file_name=os.path.basename(fp_zip)
        
        metadata_table_dict={
            "@type": "Table",
            "url": csv_file_name,
            "https://purl.org/berg/csvw_functions_extra/vocab/csv_file_name": csv_file_name,
            "https://purl.org/berg/csvw_functions_extra/vocab/zip_file_name": zip_filename, 
            "https://purl.org/berg/csvw_functions_extra/vocab/csv_zip_extract_path": csv_zip_extract_path,
            }
        
        if set_certificates and basename=='certificates.csv':
            
            metadata_table_dict.update({
                "https://purl.org/berg/csvw_functions_extra/vocab/sql_table_name":'domestic_certificates',
                'tableSchema': metadata_schema_dict_certificates
                #'tableSchema': 'https://raw.githubusercontent.com/building-energy/epc_functions/main/epc_domestic_certificates-schema-metadata.json'
                })
            
            metadata_table_group_dict['tables'].append(metadata_table_dict)
            
        if set_recommendations and basename=='recommendations.csv':
            
            metadata_table_dict.update({
                "https://purl.org/berg/csvw_functions_extra/vocab/sql_table_name":'domestic_recommendations',
                'tableSchema': metadata_schema_dict_recommendations
                #'tableSchema': 'https://raw.githubusercontent.com/building-energy/epc_functions/main/epc_domestic_recommendations-schema-metadata.json'
                })
        
            metadata_table_group_dict['tables'].append(metadata_table_dict)
        
        #break
    
    # save the newly created CSVW metadata object
    fp_out = os.path.join(
        data_folder,
        'epc_tables-metadata.json'
        )
    
    with open(fp_out,'w') as f:
        json.dump(
            metadata_table_group_dict,
            f,
            indent=4
            )
    
    return fp_out

    
def _filter_csv_file(
        fp,
        fp_out,
        start_date=None,
        end_date=None,
        var='INSPECTION_DATE'
        ):
    ""
    
    # read original csv file
    with open(fp) as f:
        
        csvreader=csv.reader(f)
        
        headers=next(csvreader)
        
        rows=[row for row in csvreader]
        
    
    # write filtered csv file
    with open(fp_out,'w',newline='') as f1:
        
        csvwriter=csv.writer(f1)
        
        #print(headers)
        
        csvwriter.writerow(headers)
        
        i=headers.index(var)
        #print(i)
        
        if not start_date is None:
            
            start_date=datetime.date.fromisoformat(start_date)
            
            if not end_date is None:
                
                end_date=datetime.date.fromisoformat(end_date)
    
                for row in rows:
                    if not row[i]=='':
                        var_date=datetime.date.fromisoformat(row[i])
                        if var_date >= start_date and var_date <= end_date:
                            csvwriter.writerow(row)
                        
            else:  # end date is None
                
                for row in rows:
                    if not row[i]=='':
                        var_date=datetime.date.fromisoformat(row[i])
                        if var_date >= start_date:
                            csvwriter.writerow(row)
                
        else:  # start date is None
        
            if not end_date is None:
                
                end_date=datetime.date.fromisoformat(end_date)
    
                for row in rows:
                    if not row[i]=='':
                        var_date=datetime.date.fromisoformat(row[i])
                        if var_date <= end_date:
                            csvwriter.writerow(row)
        
            else: # end date is None
            
                for row in csvreader:
                    csvwriter.writerow(row)
        
                        
def _import_table_group_to_sqlite(
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        csv_file_names = None, 
        verbose = False
        ):
    """
    """
    csvw_functions_extra.import_table_group_to_sqlite(
        metadata_filename = 'epc_tables-metadata.json',
        csv_file_names = csv_file_names,
        data_folder = data_folder,
        database_name = database_name,
        overwrite_existing_tables = False,
        verbose = verbose
        )



def get_epc_table_names_in_database(
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        ):
    """
    """
    
    result = \
        csvw_functions_extra.get_sql_table_names_in_database(
            data_folder = data_folder,
            database_name = database_name,
            metadata_filename = 'epc_tables-metadata.json'
            )
    
    return result
    






# def set_data_folder(
#         fp_zip=_default_fp_zip,
#         data_folder=_default_data_folder,
#         overwrite_existing_files=False,
#         database_name=_default_database_name,
#         remove_existing_tables=False,
#         inspection_date_start=None,
#         inspection_date_end=None,
#         set_certificates=True,
#         set_recommendations=True,
#         verbose=False
#         ):
#     ""
    
#     # get all csv files in epc zip file
#     csv_files = \
#         get_csv_file_names_in_zip(
#             fp_zip
#             )
    
#     # create new metadata table group object dynamically
#     metadata_table_group_dict = \
#         _get_metadata_table_group_dict(
#             csv_files,
#             fp_zip,
#             set_certificates,
#             set_recommendations
#             )
        
#     # save metadata table group object
#     metadata_document_location=os.path.join(data_folder,'epc_tables-metadata.json')
        
#     with open(metadata_document_location,'w') as f:
#         json.dump(metadata_table_group_dict,f,indent=4)
        
#     #return
    
#     # download all tables to data_folder
#     # note - this will overwrite the metadata file saved above with a normalized version
#     fp_metadata=\
#         csvw_functions_extra.download_table_group(
#             metadata_document_location,
#             data_folder=data_folder,
#             overwrite_existing_files=overwrite_existing_files,
#             verbose=verbose
#             )


#     # filter csv file
#     if not inspection_date_start is None or inspection_date_end is None:
        
#         with open(fp_metadata) as f:
#             metadata_table_group_dict=json.load(f)
        
#         for table in metadata_table_group_dict['tables']:
            
#             csv_file_name=table['https://purl.org/berg/csvw_functions_extra/vocab/csv_file_name']['@value']
#             fp_csv=os.path.join(data_folder,csv_file_name)
            
#             print(f'--- filtering {csv_file_name} ---')
            
#             if csv_file_name.endswith('certificates.csv'):
        
#                 _filter_csv_file(
#                         fp=fp_csv,
#                         fp_out=fp_csv,
#                         start_date=inspection_date_start,
#                         end_date=inspection_date_end,
#                         var='INSPECTION_DATE'
#                         )

#     #return
        
#     # import all tables to sqlite
#     csvw_functions_extra.import_table_group_to_sqlite(
#         metadata_document_location=fp_metadata,
#         data_folder=data_folder,
#         database_name=database_name,
#         remove_existing_tables=remove_existing_tables,
#         verbose=verbose
#         )


#%% main functions


def get_domestic_certificates(
        filter_by = None,  
        fields = None,  
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        pandas = False,
        verbose = False
        ):
    ""
    table_name = 'domestic_certificates'
    
    return csvw_functions_extra.get_rows(
            table_name = table_name,
            filter_by = filter_by,  
            fields = fields,  
            data_folder = data_folder,
            database_name = database_name,
            pandas = pandas,
            verbose = verbose
            )


def get_domestic_certificates_count(
        filter_by = None,
        group_by = None,
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        verbose = False
        ):
    ""
    table_name = 'domestic_certificates'
    
    return csvw_functions_extra.get_row_count(
            table_name = table_name,
            filter_by = filter_by,
            group_by = group_by,
            data_folder = data_folder,
            database_name = database_name,
            verbose = verbose
            )


def get_domestic_certificates_field_names(
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        verbose = False
        ):
    ""
    table_name = 'domestic_certificates'
    
    return csvw_functions_extra.get_field_names(
            table_name = table_name,
            data_folder = data_folder,
            database_name = database_name,
            verbose = verbose
            )


def get_domestic_recommendations(
        filter_by = None,  
        fields = None, 
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        verbose = False
        ):
    ""
    table_name = 'domestic_recommendations'
    
    return csvw_functions_extra.get_rows(
            table_name = table_name,
            filter_by = filter_by,  
            fields = fields,  
            data_folder = data_folder,
            database_name = database_name,
            verbose = verbose
            )


def get_domestic_recommendations_count(
        filter_by = None,
        group_by = None,
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        verbose = False
        ):
    ""
    table_name = 'domestic_recommendations'
    
    return csvw_functions_extra.get_row_count(
            table_name = table_name,
            filter_by = filter_by,
            group_by = group_by,
            data_folder = data_folder,
            database_name = database_name,
            verbose = verbose
            )


def get_domestic_recommendations_field_names(
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        ):
    ""
    table_name = 'domestic_recommendations'
    
    return csvw_functions_extra.get_field_names(
            table_name = table_name,
            data_folder = data_folder,
            database_name = database_name,
            )







# def get_table_names(
#         data_folder=_default_data_folder,
#         database_name=_default_database_name
#         ):
#     ""
#     fp_database=os.path.join(data_folder,database_name)
#     with sqlite3.connect(fp_database) as conn:
#         c = conn.cursor()
#         query="SELECT * FROM sqlite_master WHERE type='table';"
#         return [x[1] for x in c.execute(query).fetchall()]
    
    

    





# def get_list_of_dates(
#         start_date,  # i.e. 2021-01-01  %Y-%m-%d
#         number_of_days
#         ):
#     ""
#     start=datetime.datetime.strptime(start_date, '%Y-%m-%d')
#     return [(start + datetime.timedelta(days=x)).strftime('%Y-%m-%d') 
#             for x in range(number_of_days)]
    



# #%% read metadata files

# # epc_domestic_certificates_schema_metadata
# fp=os.path.join(
#     pkg_resources.files(epc_functions),
#     'epc_domestic_certificates-schema-metadata.json'
#     )
# #print(fp)
# with open(fp) as f:
#     epc_domestic_certificates_schema_metadata=json.load(f)
# #print(epc_domestic_certificates_schema_metadata)

# # epc_domestic_recommendations_schema_metadata
# fp=os.path.join(
#     pkg_resources.files(epc_functions),
#     'epc_domestic_recommendations-schema-metadata.json'
#     )
# #print(fp)
# with open(fp) as f:
#     epc_domestic_recommendations_schema_metadata=json.load(f)
# #print(epc_domestic_recommendations_schema_metadata)


# #%% database functions

# def create_epc_domestic_certificates_table(
#         fp_database,
#         verbose=True
#         ):
#     """Creates a new epc domestic certificates table in the sqlite database.
    
#     No action if table already exists.
    
#     """
    
#     if _check_if_table_exists_in_database(
#             fp_database,
#             'epc_domestic_certificates'
#             ):
#         if verbose:
#             print('---CREATE TABLE---')
#             print('Table "epc_domestic_certificates" already exists in database - no action')
#         return
    
    
#     # create query
#     datatype_map={
#     'integer':'INTEGER',
#     'decimal':'REAL'
#     }
#     query='CREATE TABLE epc_domestic_certificates ('
    
#     for column_dict in epc_domestic_certificates_schema_metadata['columns']:
#         name=column_dict['name']
#         datatype=datatype_map.get(column_dict['datatype']['base'],'TEXT')
#         query+=f"{name} {datatype}"
#         query+=", "
#     query=query[:-2]
        
#     if 'primaryKey' in epc_domestic_certificates_schema_metadata:
        
#         pk=epc_domestic_certificates_schema_metadata['primaryKey']
#         if isinstance(pk,str):
#             pk=[pk]
#         query+=', PRIMARY KEY ('
#         for x in pk:
#             query+=x
#             query+=", "
#         query=query[:-2]
#         query+=') '
        
#     query+=');'
    
#     if verbose:
#         print('---QUERY TO CREATE TABLE---')
#         print(query)
    
#     # create table in database
#     with sqlite3.connect(fp_database) as conn:
#         c = conn.cursor()
        
#         # create table
#         c.execute(query)
#         conn.commit()
        
        
    
# def create_epc_domestic_recommendations_table(
#         fp_database,
#         verbose=True
#         ):
#     """Creates a new epc domestic recommendations table in the sqlite database.
    
#     No action if table already exists.
    
#     """
#     if _check_if_table_exists_in_database(
#             fp_database,
#             'epc_domestic_recommendations'
#             ):
#         if verbose:
#             print('---CREATE TABLE---')
#             print('Table "epc_domestic_recommendations" already exists in database - no action')
#         return
    
    
#     datatype_map={
#     'integer':'INTEGER',
#     'decimal':'REAL'
#     }
#     query='CREATE TABLE epc_domestic_recommendations ('
#     for column_dict in epc_domestic_recommendations_schema_metadata['columns']:
#         name=column_dict['name']
#         datatype=datatype_map.get(column_dict['datatype']['base'],'TEXT')
#         query+=f"{name} {datatype}"
#         query+=", "
#     query=query[:-2]
        
#     if 'primaryKey' in epc_domestic_recommendations_schema_metadata:
        
#         pk=epc_domestic_recommendations_schema_metadata['primaryKey']
#         if isinstance(pk,str):
#             pk=[pk]
#         query+=', PRIMARY KEY ('
#         for x in pk:
#             query+=x
#             query+=", "
#         query=query[:-2]
#         query+=') '
        
#     query+=');'
    
#     if verbose:
#         print('---QUERY TO CREATE TABLE---')
#         print(query)
    
#     # create table in database
#     with sqlite3.connect(fp_database) as conn:
#         c = conn.cursor()
        
#         # delete table if already exists
#         c.execute("DROP TABLE IF EXISTS epc_domestic_recommendations;")
#         conn.commit()
    
#         # create table
#         c.execute(query)
#         conn.commit()
        
#         # list tables in database
#         query="SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
#         #print([x[0] for x in c.execute(query).fetchall()])
    

# def import_epc_domestic_certificates_csv_file(
#         fp_database,
#         fp_csv,
#         verbose=True
#         ):
#     """
#     """
#     fp_database=fp_database.replace('\\','\\\\')
#     fp_csv=fp_csv.replace('\\','\\\\')
#     command=f'sqlite3 {fp_database} -cmd ".mode csv" ".import --skip 1 {fp_csv} epc_domestic_certificates"'
#     if verbose:
#         print('---COMMAND LINE TO IMPORT DATA---')
#         print('Number of rows before import:', _get_row_count_in_database_table(fp_database,'epc_domestic_certificates','LMK_KEY'))
#         print(command)
#     subprocess.run(command)
#     if verbose:
#         print('Number of rows after import: ', _get_row_count_in_database_table(fp_database,'epc_domestic_certificates','LMK_KEY'))
    
    
# def import_epc_domestic_recommendations_csv_file(
#         fp_database,
#         fp_csv,
#         verbose=True
#         ):
#     """
#     """
#     fp_database=fp_database.replace('\\','\\\\')
#     fp_csv=fp_csv.replace('\\','\\\\')
#     command=f'sqlite3 {fp_database} -cmd ".mode csv" ".import --skip 1 {fp_csv} epc_domestic_recommendations"'
#     if verbose:
#         print('---COMMAND LINE TO IMPORT DATA---')
#         print('Number of rows before import:', _get_row_count_in_database_table(fp_database,'epc_domestic_recommendations','LMK_KEY'))
#         print(command)
#     subprocess.run(command)
#     if verbose:
#         print('Number of rows after import: ', _get_row_count_in_database_table(fp_database,'epc_domestic_recommendations','LMK_KEY'))
    
    
# # def _find_all_certificates_csv_files_in_folder(
# #         fp_folder
# #         ):
# #     """Finds all 'certificates.csv' files in the folder.
    
# #     Searches the folder and all subfolders.
    
# #     """
# #     result = []
# #     for root, dirnames, filenames in os.walk(fp_folder):
# #         if 'certificates.csv' in filenames:
# #             result.append(
# #                 os.path.join(
# #                     root,
# #                     'certificates.csv'
# #                     )
# #                 )
# #     return result
        

    
# def find_all_recommendations_csv_files_in_folder(
#         fp_folder
#         ):
#     """Finds all 'recommendations.csv' files in the folder.
    
#     Searches the folder and all subfolders.
    
#     """
#     result = []
#     for root, dirnames, filenames in os.walk(fp_folder):
#         if 'recommendations.csv' in filenames:
#             result.append(
#                 os.path.join(
#                     root,
#                     'recommendations.csv'
#                     )
#                 )
#     return result
    

# def import_all_epc_domestic_csv_files_in_folder(
#         fp_database,
#         fp_folder,
#         verbose=True
#         ):
#     """
    
#     Imports 'certificates.csv' and 'recommendations.csv' files.
    
#     """
    
#     # --- certificates ---
    
#     # create certificates table if needed
#     create_epc_domestic_certificates_table(
#         fp_database,
#         verbose=verbose
#         )
    
#     # import all certificates files into database
#     for fp_csv in find_all_certificates_csv_files_in_folder(
#             fp_folder
#             ):
        
#         import_epc_domestic_certificates_csv_file(
#             fp_database,
#             fp_csv,
#             verbose=verbose            
#             )
        
#     # --- recommendations ---
#     # create recommendations table if needed
#     create_epc_domestic_recommendations_table(
#         fp_database,
#         verbose=verbose
#         )
    
#     # import all recommendations files into database
#     for fp_csv in find_all_recommendations_csv_files_in_folder(
#             fp_folder
#             ):
        
#         import_epc_domestic_recommendations_csv_file(
#             fp_database,
#             fp_csv,
#             verbose=verbose            
#             )
    
    
    
    

    
# def _check_if_table_exists_in_database(
#         fp_database,
#         table_name
#         ):
#     ""
#     with sqlite3.connect(fp_database) as conn:
#         c = conn.cursor()
#         query=f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table_name}';"
#         return True if c.execute(query).fetchall()[0][0] else False
    

    
    