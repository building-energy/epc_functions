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

fp_database='epc_test.sqlite'


#%% set up utility functions

def _list_tables_in_database(
        fp_database
        ):
    """
    
    """
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query="SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
        return [x[0] for x in c.execute(query).fetchall()]
    
    
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


#%% TestCases

class TestDataFolder(unittest.TestCase):
    ""
    
    def test_set_data_folder(self):
        ""
        
        fp_zip=r'C:\Users\cvskf\OneDrive - Loughborough University\_Data\Energy_Performance_Certificates\2023-08\all-domestic-certificates.zip'
        
        epc_functions.set_data_folder(
            fp_zip=fp_zip,
            #overwrite_existing_files=True,
            #remove_existing_tables=True,
            verbose=True
            )
        


class TestEPCFunctions(unittest.TestCase):
    ""
    
    def _test_1_create_epc_domestic_certificates_table(self):
        ""
        
        epc_functions.create_epc_domestic_certificates_table(fp_database)
        
        self.assertIn(
            'epc_domestic_certificates', 
            _list_tables_in_database(fp_database)
            )
        
        
    def _test_2_create_epc_domestic_recommendations_table(self):
        ""
        
        epc_functions.create_epc_domestic_recommendations_table(fp_database)

        self.assertIn(
            'epc_domestic_recommendations', 
            _list_tables_in_database(fp_database)
            )
        
        
    def _test_3_import_epc_domestic_certificates_csv_file(self):
        ""
        fp_csv=os.path.join(
            'all-domestic-certificates',
            'domestic-E07000130-Charnwood',
            'certificates.csv'
            )
        
        epc_functions.import_epc_domestic_certificates_csv_file(
            fp_database, 
            fp_csv
            )
        
        self.assertEqual(
            _get_row_count_in_database_table(
                    fp_database,
                    'epc_domestic_certificates'
                    ),
            1000
            )
        
        # duplication - but should not increase the row count
        epc_functions.import_epc_domestic_certificates_csv_file(
            fp_database, 
            fp_csv
            )
        
        self.assertEqual(
            _get_row_count_in_database_table(
                    fp_database,
                    'epc_domestic_certificates'
                    ),
            1000
            )
        
    def _test_4_import_epc_domestic_recommendations_csv_file(self):
        ""
        fp_csv=os.path.join(
            'all-domestic-certificates',
            'domestic-E07000130-Charnwood',
            'recommendations.csv'
            )
        
        epc_functions.import_epc_domestic_recommendations_csv_file(
            fp_database, 
            fp_csv
            )
        
        self.assertEqual(
            _get_row_count_in_database_table(
                    fp_database,
                    'epc_domestic_recommendations'
                    ),
            1000  
            )
        
        # duplication - but should not increase the row count
        epc_functions.import_epc_domestic_recommendations_csv_file(
            fp_database, 
            fp_csv
            )
        
        self.assertEqual(
            _get_row_count_in_database_table(
                    fp_database,
                    'epc_domestic_recommendations'
                    ),
            1000
            )
        
        
    def _test_5_find_all_certificates_csv_files_in_folder(self):
        ""
        fp_folder='all-domestic-certificates'
        
        result=\
            epc_functions.find_all_certificates_csv_files_in_folder(
                fp_folder
                )
            
        self.assertEqual(
            result,
            ['all-domestic-certificates\\domestic-E07000130-Charnwood\\certificates.csv']
            )
            
        
    def _test_6_find_all_recommendations_csv_files_in_folder(self):
        ""
        fp_folder='all-domestic-certificates'
        
        result=\
            epc_functions.find_all_recommendations_csv_files_in_folder(
                fp_folder
                )
            
        self.assertEqual(
            result,
            ['all-domestic-certificates\\domestic-E07000130-Charnwood\\recommendations.csv']
            )
            
        
        
    def _test_7_import_all_epc_domestic_csv_files_in_folder(self):
        ""
        fp_folder='all-domestic-certificates'
        
        epc_functions.import_all_epc_domestic_csv_files_in_folder(
            fp_database,
            fp_folder,
            verbose=True
            )
        
        self.assertEqual(
            _get_row_count_in_database_table(
                    fp_database,
                    'epc_domestic_certificates'
                    ),
            1000
            )
        
        self.assertEqual(
            _get_row_count_in_database_table(
                    fp_database,
                    'epc_domestic_recommendations'
                    ),
            1000
            )
        
        
        
#%% run unittests

if __name__ == '__main__':
    
    unittest.main()
