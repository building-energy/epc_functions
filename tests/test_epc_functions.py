# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 14:48:40 2023

@author: cvskf
"""

#%% import packages

import unittest
from epc_functions import epc_functions
import sqlite3
import os


#%% set up filepaths

# data_folder=os.path.join(
#     os.pardir,
#     os.pardir,
#     'Energy_Performance_Certificates_dataset',
#     '_data')



#%% TestCases

class TestDataFolder(unittest.TestCase):
    ""
    
    def test_get_csv_zip_extract_paths_in_zip(self):
        ""
        result = \
            epc_functions.get_csv_zip_extract_paths_in_zip(
                )
        #print(len(result))
        #print(result[0])
        
        self.assertEqual(
            len(result),
            694
            )
    
        self.assertEqual(
            result[0],
            'domestic-E07000044-South-Hams/certificates.csv'
            )
        
    def test_extract_and_import_data(self):
        ""
        epc_functions.extract_and_import_data(
            csv_zip_extract_paths = 'domestic-E07000044-South-Hams/certificates.csv',
            inspection_date_start='2021-01-01',
            inspection_date_end='2021-12-31',
            verbose = True
            )
        
        
    def test__extract_table_group(self):
        ""
        epc_functions._extract_table_group(
            csv_zip_extract_paths = 'domestic-E07000044-South-Hams/certificates.csv',
            inspection_date_start='2021-01-01',
            inspection_date_end='2021-12-31',
            verbose = False
            )
        
        
    def test__import_table_group_to_sqlite(self):
        ""
        epc_functions._import_table_group_to_sqlite(
            verbose = False
            )
        

    def test___create_metadata_table_group_file_pre_file_extraction(self):
        ""
        result = \
            epc_functions._create_metadata_table_group_file_pre_file_extraction(
                csv_zip_extract_paths = 'domestic-E07000044-South-Hams/certificates.csv'
                )
        #print(result)
        self.assertEqual(
            result,
            '_data\epc_tables-metadata.json'
            )
        
    
    
    def test_filter_csv_file(self):
        ""
        epc_functions._filter_csv_file(
            fp=r'C:\Users\cvskf\OneDrive - Loughborough University\_Git\building-energy\Energy_Performance_Certificates_dataset\_data\domestic-E06000001-Hartlepool_certificates.csv',
            fp_out='test.csv',
            start_date='2021-01-01',
            end_date='2021-12-31'
            )
        
    
    
    
    def _test_set_data_folder(self):
        ""
        
        
        epc_functions.set_data_folder(
            fp_zip=fp_zip,
            #overwrite_existing_files=True,
            #remove_existing_tables=True,
            data_folder=data_folder,
            verbose=True
            )
        

class TestMainFunctions(unittest.TestCase):
    ""
    
    def _test_get_table_names(self):
        ""
        
        result=epc_functions.get_table_names(data_folder=data_folder)
        #print(result)
        

    def _test_get_field_names(self):
        ""
        
        result=epc_functions.get_field_names(
            'domestic_certificates',
            data_folder=data_folder
            )
        #print(result)
        
        
        
    def _test_get_row_count(self):
        ""
        table_name='domestic_certificates'
        verbose=False
        
        filter_by={'POSTCODE':'LE11 3PF'}
        result=epc_functions.get_row_count(table_name,filter_by=filter_by,data_folder=data_folder,verbose=verbose)
        #print(result)
        self.assertEqual(result,[{'COUNT': 3}])
        
        filter_by={'LOCAL_AUTHORITY':'E07000130'}
        group_by=['PROPERTY_TYPE']
        result=epc_functions.get_row_count(table_name,filter_by=filter_by,group_by=group_by,data_folder=data_folder,verbose=verbose)
        #print(result)
        self.assertEqual(
            result,
            [{'PROPERTY_TYPE': 'Bungalow', 'COUNT': 6445}, 
             {'PROPERTY_TYPE': 'Flat', 'COUNT': 11066}, 
             {'PROPERTY_TYPE': 'House', 'COUNT': 49495}, 
             {'PROPERTY_TYPE': 'Maisonette', 'COUNT': 1082}, 
             {'PROPERTY_TYPE': 'Park home', 'COUNT': 8}]
            )
        
        filter_by={'LOCAL_AUTHORITY':'E07000130'}
        group_by=['PROPERTY_TYPE','BUILT_FORM']
        result=epc_functions.get_row_count(table_name,filter_by=filter_by,group_by=group_by,data_folder=data_folder,verbose=verbose)
        #print(len(result))
        self.assertEqual(len(result),30)
        
        
        
        
    def _test_get_rows(self):
        ""
        table_name='domestic_certificates'
        verbose=False
        
        filter_by={'POSTCODE':'LE11 3PF'}
        result=epc_functions.get_rows(table_name,filter_by,data_folder=data_folder,verbose=verbose)
        #print(len(result))
        self.assertEqual(len(result),3)
        #print(result[2])
        
        fields='LMK_KEY'
        filter_by={'POSTCODE':'LE11 3PF'}
        result=epc_functions.get_rows(table_name,filter_by,fields=fields,data_folder=data_folder,verbose=verbose)
        #print(len(result))
        self.assertEqual(len(result),3)
        
        fields=''
        filter_by={'POSTCODE':'LE11 3PF'}
        result=epc_functions.get_rows(table_name,filter_by,fields=fields,data_folder=data_folder,verbose=verbose)
        #print(len(result))
        self.assertEqual(len(result),3)
        
        fields=['LMK_KEY','UPRN']
        filter_by={'POSTCODE':'LE11 3PF'}
        result=epc_functions.get_rows(table_name,filter_by,fields=fields,data_folder=data_folder,verbose=verbose)
        #print(len(result))
        self.assertEqual(len(result),3)
        
        
        filter_by={'POSTCODE':['LE11 3PE','LE11 3PF']}
        result=epc_functions.get_rows(table_name,filter_by,data_folder=data_folder,verbose=verbose)
        #print(len(result))
        self.assertEqual(len(result),21)
        
        filter_by={'INSPECTION_DATE':'2021-01-01'}
        result=epc_functions.get_rows(table_name,filter_by,data_folder=data_folder,verbose=verbose)
        #print(len(result))
        self.assertEqual(len(result),85)
        
        fields='LMK_KEY'
        filter_by={'INSPECTION_DATE':{'BETWEEN':['2021-01-01','2021-01-02']}}
        result=epc_functions.get_rows(table_name,filter_by,fields=fields,data_folder=data_folder,verbose=verbose)
        #print(len(result))
        self.assertEqual(len(result),335)
        
        
        
    


# class TestEPCFunctions(unittest.TestCase):
#     ""
    
#     def _test_1_create_epc_domestic_certificates_table(self):
#         ""
        
#         epc_functions.create_epc_domestic_certificates_table(fp_database)
        
#         self.assertIn(
#             'epc_domestic_certificates', 
#             _list_tables_in_database(fp_database)
#             )
        
        
#     def _test_2_create_epc_domestic_recommendations_table(self):
#         ""
        
#         epc_functions.create_epc_domestic_recommendations_table(fp_database)

#         self.assertIn(
#             'epc_domestic_recommendations', 
#             _list_tables_in_database(fp_database)
#             )
        
        
#     def _test_3_import_epc_domestic_certificates_csv_file(self):
#         ""
#         fp_csv=os.path.join(
#             'all-domestic-certificates',
#             'domestic-E07000130-Charnwood',
#             'certificates.csv'
#             )
        
#         epc_functions.import_epc_domestic_certificates_csv_file(
#             fp_database, 
#             fp_csv
#             )
        
#         self.assertEqual(
#             _get_row_count_in_database_table(
#                     fp_database,
#                     'epc_domestic_certificates'
#                     ),
#             1000
#             )
        
#         # duplication - but should not increase the row count
#         epc_functions.import_epc_domestic_certificates_csv_file(
#             fp_database, 
#             fp_csv
#             )
        
#         self.assertEqual(
#             _get_row_count_in_database_table(
#                     fp_database,
#                     'epc_domestic_certificates'
#                     ),
#             1000
#             )
        
#     def _test_4_import_epc_domestic_recommendations_csv_file(self):
#         ""
#         fp_csv=os.path.join(
#             'all-domestic-certificates',
#             'domestic-E07000130-Charnwood',
#             'recommendations.csv'
#             )
        
#         epc_functions.import_epc_domestic_recommendations_csv_file(
#             fp_database, 
#             fp_csv
#             )
        
#         self.assertEqual(
#             _get_row_count_in_database_table(
#                     fp_database,
#                     'epc_domestic_recommendations'
#                     ),
#             1000  
#             )
        
#         # duplication - but should not increase the row count
#         epc_functions.import_epc_domestic_recommendations_csv_file(
#             fp_database, 
#             fp_csv
#             )
        
#         self.assertEqual(
#             _get_row_count_in_database_table(
#                     fp_database,
#                     'epc_domestic_recommendations'
#                     ),
#             1000
#             )
        
        
#     def _test_5_find_all_certificates_csv_files_in_folder(self):
#         ""
#         fp_folder='all-domestic-certificates'
        
#         result=\
#             epc_functions.find_all_certificates_csv_files_in_folder(
#                 fp_folder
#                 )
            
#         self.assertEqual(
#             result,
#             ['all-domestic-certificates\\domestic-E07000130-Charnwood\\certificates.csv']
#             )
            
        
#     def _test_6_find_all_recommendations_csv_files_in_folder(self):
#         ""
#         fp_folder='all-domestic-certificates'
        
#         result=\
#             epc_functions.find_all_recommendations_csv_files_in_folder(
#                 fp_folder
#                 )
            
#         self.assertEqual(
#             result,
#             ['all-domestic-certificates\\domestic-E07000130-Charnwood\\recommendations.csv']
#             )
            
        
        
#     def _test_7_import_all_epc_domestic_csv_files_in_folder(self):
#         ""
#         fp_folder='all-domestic-certificates'
        
#         epc_functions.import_all_epc_domestic_csv_files_in_folder(
#             fp_database,
#             fp_folder,
#             verbose=True
#             )
        
#         self.assertEqual(
#             _get_row_count_in_database_table(
#                     fp_database,
#                     'epc_domestic_certificates'
#                     ),
#             1000
#             )
        
#         self.assertEqual(
#             _get_row_count_in_database_table(
#                     fp_database,
#                     'epc_domestic_recommendations'
#                     ),
#             1000
#             )
        
        

# #%% set up utility functions

# def _list_tables_in_database(
#         fp_database
#         ):
#     """
    
#     """
#     with sqlite3.connect(fp_database) as conn:
#         c = conn.cursor()
#         query="SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
#         return [x[0] for x in c.execute(query).fetchall()]
    
    
# def _get_row_count_in_database_table(
#         fp_database,
#         table_name
#         ):
#     """Gets number of rows in table
    
#     """
#     with sqlite3.connect(fp_database) as conn:
#         c = conn.cursor()
#         query=f'SELECT COUNT(*) FROM {table_name}'
#         return c.execute(query).fetchone()[0]
        
        
#%% run unittests

if __name__ == '__main__':
    
    unittest.main()
