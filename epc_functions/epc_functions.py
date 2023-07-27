# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 14:42:31 2023

@author: cvskf
"""


#%% import packages

import sqlite3
import csv, os, time, json
import importlib.resources as pkg_resources
import epc_functions
import subprocess



#%% read metadata files

# epc_domestic_certificates_schema_metadata
fp=os.path.join(
    pkg_resources.files(epc_functions),
    'epc_domestic_certificates-schema-metadata.json'
    )
#print(fp)
with open(fp) as f:
    epc_domestic_certificates_schema_metadata=json.load(f)
#print(epc_domestic_certificates_schema_metadata)

# epc_domestic_recommendations_schema_metadata
fp=os.path.join(
    pkg_resources.files(epc_functions),
    'epc_domestic_recommendations-schema-metadata.json'
    )
#print(fp)
with open(fp) as f:
    epc_domestic_recommendations_schema_metadata=json.load(f)
#print(epc_domestic_recommendations_schema_metadata)


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
        print('Number of rows before import:', _get_row_count_in_database_table(fp_database,'epc_domestic_certificates'))
        print(command)
    subprocess.run(command)
    if verbose:
        print('Number of rows after import: ', _get_row_count_in_database_table(fp_database,'epc_domestic_certificates'))
    
    
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
        print('Number of rows before import:', _get_row_count_in_database_table(fp_database,'epc_domestic_recommendations'))
        print(command)
    subprocess.run(command)
    if verbose:
        print('Number of rows after import: ', _get_row_count_in_database_table(fp_database,'epc_domestic_recommendations'))
    
    
def find_all_certificates_csv_files_in_folder(
        fp_folder
        ):
    """Finds all 'certificates.csv' files in the folder.
    
    Searches the folder and all subfolders.
    
    """
    result = []
    for root, dirnames, filenames in os.walk(fp_folder):
        if 'certificates.csv' in filenames:
            result.append(
                os.path.join(
                    root,
                    'certificates.csv'
                    )
                )
    return result
        
    
    
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
        table_name
        ):
    """Gets number of rows in table
    
    """
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query=f'SELECT COUNT(*) FROM {table_name}'
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
    
    
    