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
from csvw_functions import csvw_functions, csvw_functions_extra
import zipfile

_default_data_folder='_data'  # the default
_default_database_name='epc_data.sqlite'

urllib.request.urlcleanup()



#%% data folder



def _get_csv_files_in_zip(
        fp_zip
        ):
    ""
    z = zipfile.ZipFile(fp_zip)
    
    result=z.namelist()
    
    result=[x for x in result if os.path.splitext(x)[1]=='.csv']
    
    return result


def _get_metadata_table_group_dict(
        csv_files,
        fp_zip
        ):
    ""
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
    
    for csv_file in csv_files:
        
        basename=os.path.basename(csv_file)
        #print('basename', basename)
        dirname=os.path.dirname(csv_file)
        #print('dirname', dirname)
        
        if not basename in ['certificates.csv','recommendations.csv']:
            continue
        
        csv_file_name=f'{dirname}_{basename}'
        
        metadata_table_dict={
            "@type": "Table",
            "url": fp_zip,
            "https://purl.org/berg/csvw_functions/vocab/csv_file_name": csv_file_name,
            "https://purl.org/berg/csvw_functions/vocab/zip_filename": fp_zip, 
            "https://purl.org/berg/csvw_functions/vocab/csv_zip_extract_path": csv_file,
            }
        
        if basename=='certificates.csv':
            
            metadata_table_dict.update({
                "https://purl.org/berg/csvw_functions/vocab/sql_table_name":'domestic_certificates',
                'tableSchema': metadata_schema_dict_certificates
                })
            
        elif basename=='recommendations.csv':
            
            metadata_table_dict.update({
                "https://purl.org/berg/csvw_functions/vocab/sql_table_name":'domestic_recommendations',
                'tableSchema': metadata_schema_dict_recommendations
                })
        
        else:
            
            raise Exception
        
        metadata_table_group_dict['tables'].append(metadata_table_dict)
        
        #break
    

    return metadata_table_group_dict

    

def set_data_folder(
        fp_zip,
        data_folder=_default_data_folder,
        overwrite_existing_files=False,
        database_name=_default_database_name,
        remove_existing_tables=False,
        verbose=False
        ):
    ""
    
    # get all csv files in epc zip file
    csv_files = \
        _get_csv_files_in_zip(
            fp_zip
            )
    
    # create new metadata table group object dynamically
    metadata_table_group_dict = \
        _get_metadata_table_group_dict(
            csv_files,
            fp_zip
            )
        
    # save metadata table group object
    metadata_document_location=os.path.join(data_folder,'epc_tables-metadata.json')
        
    with open(metadata_document_location,'w') as f:
        json.dump(metadata_table_group_dict,f,indent=4)
        
    #return
    
    # download all tables to data_folder
    # note - this will overwrite the metadata file saved above with a normalized version
    fp_metadata=\
        csvw_functions_extra.download_table_group(
            metadata_document_location,
            data_folder=data_folder,
            overwrite_existing_files=overwrite_existing_files,
            verbose=verbose
            )

    #return
        
    # import all tables to sqlite
    csvw_functions_extra.import_table_group_to_sqlite(
        metadata_document_location=fp_metadata,
        data_folder=data_folder,
        database_name=database_name,
        remove_existing_tables=remove_existing_tables,
        verbose=verbose
        )





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


#%% database functions

def create_epc_domestic_certificates_table(
        fp_database,
        verbose=True
        ):
    """Creates a new epc domestic certificates table in the sqlite database.
    
    No action if table already exists.
    
    """
    
    if _check_if_table_exists_in_database(
            fp_database,
            'epc_domestic_certificates'
            ):
        if verbose:
            print('---CREATE TABLE---')
            print('Table "epc_domestic_certificates" already exists in database - no action')
        return
    
    
    # create query
    datatype_map={
    'integer':'INTEGER',
    'decimal':'REAL'
    }
    query='CREATE TABLE epc_domestic_certificates ('
    
    for column_dict in epc_domestic_certificates_schema_metadata['columns']:
        name=column_dict['name']
        datatype=datatype_map.get(column_dict['datatype']['base'],'TEXT')
        query+=f"{name} {datatype}"
        query+=", "
    query=query[:-2]
        
    if 'primaryKey' in epc_domestic_certificates_schema_metadata:
        
        pk=epc_domestic_certificates_schema_metadata['primaryKey']
        if isinstance(pk,str):
            pk=[pk]
        query+=', PRIMARY KEY ('
        for x in pk:
            query+=x
            query+=", "
        query=query[:-2]
        query+=') '
        
    query+=');'
    
    if verbose:
        print('---QUERY TO CREATE TABLE---')
        print(query)
    
    # create table in database
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        
        # create table
        c.execute(query)
        conn.commit()
        
        
    
def create_epc_domestic_recommendations_table(
        fp_database,
        verbose=True
        ):
    """Creates a new epc domestic recommendations table in the sqlite database.
    
    No action if table already exists.
    
    """
    if _check_if_table_exists_in_database(
            fp_database,
            'epc_domestic_recommendations'
            ):
        if verbose:
            print('---CREATE TABLE---')
            print('Table "epc_domestic_recommendations" already exists in database - no action')
        return
    
    
    datatype_map={
    'integer':'INTEGER',
    'decimal':'REAL'
    }
    query='CREATE TABLE epc_domestic_recommendations ('
    for column_dict in epc_domestic_recommendations_schema_metadata['columns']:
        name=column_dict['name']
        datatype=datatype_map.get(column_dict['datatype']['base'],'TEXT')
        query+=f"{name} {datatype}"
        query+=", "
    query=query[:-2]
        
    if 'primaryKey' in epc_domestic_recommendations_schema_metadata:
        
        pk=epc_domestic_recommendations_schema_metadata['primaryKey']
        if isinstance(pk,str):
            pk=[pk]
        query+=', PRIMARY KEY ('
        for x in pk:
            query+=x
            query+=", "
        query=query[:-2]
        query+=') '
        
    query+=');'
    
    if verbose:
        print('---QUERY TO CREATE TABLE---')
        print(query)
    
    # create table in database
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        
        # delete table if already exists
        c.execute("DROP TABLE IF EXISTS epc_domestic_recommendations;")
        conn.commit()
    
        # create table
        c.execute(query)
        conn.commit()
        
        # list tables in database
        query="SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
        #print([x[0] for x in c.execute(query).fetchall()])
    

def import_epc_domestic_certificates_csv_file(
        fp_database,
        fp_csv,
        verbose=True
        ):
    """
    """
    fp_database=fp_database.replace('\\','\\\\')
    fp_csv=fp_csv.replace('\\','\\\\')
    command=f'sqlite3 {fp_database} -cmd ".mode csv" ".import --skip 1 {fp_csv} epc_domestic_certificates"'
    if verbose:
        print('---COMMAND LINE TO IMPORT DATA---')
        print('Number of rows before import:', _get_row_count_in_database_table(fp_database,'epc_domestic_certificates','LMK_KEY'))
        print(command)
    subprocess.run(command)
    if verbose:
        print('Number of rows after import: ', _get_row_count_in_database_table(fp_database,'epc_domestic_certificates','LMK_KEY'))
    
    
def import_epc_domestic_recommendations_csv_file(
        fp_database,
        fp_csv,
        verbose=True
        ):
    """
    """
    fp_database=fp_database.replace('\\','\\\\')
    fp_csv=fp_csv.replace('\\','\\\\')
    command=f'sqlite3 {fp_database} -cmd ".mode csv" ".import --skip 1 {fp_csv} epc_domestic_recommendations"'
    if verbose:
        print('---COMMAND LINE TO IMPORT DATA---')
        print('Number of rows before import:', _get_row_count_in_database_table(fp_database,'epc_domestic_recommendations','LMK_KEY'))
        print(command)
    subprocess.run(command)
    if verbose:
        print('Number of rows after import: ', _get_row_count_in_database_table(fp_database,'epc_domestic_recommendations','LMK_KEY'))
    
    
# def _find_all_certificates_csv_files_in_folder(
#         fp_folder
#         ):
#     """Finds all 'certificates.csv' files in the folder.
    
#     Searches the folder and all subfolders.
    
#     """
#     result = []
#     for root, dirnames, filenames in os.walk(fp_folder):
#         if 'certificates.csv' in filenames:
#             result.append(
#                 os.path.join(
#                     root,
#                     'certificates.csv'
#                     )
#                 )
#     return result
        

    
def find_all_recommendations_csv_files_in_folder(
        fp_folder
        ):
    """Finds all 'recommendations.csv' files in the folder.
    
    Searches the folder and all subfolders.
    
    """
    result = []
    for root, dirnames, filenames in os.walk(fp_folder):
        if 'recommendations.csv' in filenames:
            result.append(
                os.path.join(
                    root,
                    'recommendations.csv'
                    )
                )
    return result
    

def import_all_epc_domestic_csv_files_in_folder(
        fp_database,
        fp_folder,
        verbose=True
        ):
    """
    
    Imports 'certificates.csv' and 'recommendations.csv' files.
    
    """
    
    # --- certificates ---
    
    # create certificates table if needed
    create_epc_domestic_certificates_table(
        fp_database,
        verbose=verbose
        )
    
    # import all certificates files into database
    for fp_csv in find_all_certificates_csv_files_in_folder(
            fp_folder
            ):
        
        import_epc_domestic_certificates_csv_file(
            fp_database,
            fp_csv,
            verbose=verbose            
            )
        
    # --- recommendations ---
    # create recommendations table if needed
    create_epc_domestic_recommendations_table(
        fp_database,
        verbose=verbose
        )
    
    # import all recommendations files into database
    for fp_csv in find_all_recommendations_csv_files_in_folder(
            fp_folder
            ):
        
        import_epc_domestic_recommendations_csv_file(
            fp_database,
            fp_csv,
            verbose=verbose            
            )
    
    
    
    


    
    
def _get_row_count_in_database_table(
        fp_database,
        table_name,
        column_name='*'
        ):
    """Gets number of rows in table
    
    """
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query=f'SELECT COUNT({column_name}) FROM {table_name}'
        return c.execute(query).fetchone()[0]
    

def _check_if_table_exists_in_database(
        fp_database,
        table_name
        ):
    ""
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query=f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        return True if c.execute(query).fetchall()[0][0] else False
    
    
    