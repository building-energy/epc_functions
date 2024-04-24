# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 14:48:40 2023

@author: cvskf
"""

import unittest
from epc_functions import epc_functions
import sqlite3
import os


class TestDataFolder(unittest.TestCase):
    ""
    
    def test_get_csv_zip_extract_paths_in_zip(self):
        ""
        result = \
            epc_functions.get_csv_zip_extract_paths_in_zip(
                zip_filepath = '_data/all-domestic-certificates.zip'
                )
        
        self.assertEqual(
            len(result),
            694
            )
    
        self.assertEqual(
            result[0],
            'domestic-E07000044-South-Hams/certificates.csv'
            )
        
        
    def test_create_metadata_table_group_file(self):
        ""
        metadata_table_group_dict = \
            epc_functions.create_metadata_table_group_file(
                metadata_filepath = 'epc_tables-metadata.json',
                zip_filepath = '_data/all-domestic-certificates.zip',
                csv_zip_extract_paths = [
                    'domestic-E07000044-South-Hams/certificates.csv',
                    'domestic-E07000044-South-Hams/recommendations.csv'
                    ],
                )
        
        self.assertTrue(isinstance(metadata_table_group_dict,dict))
        self.assertTrue(os.path.isfile('epc_tables-metadata.json'))  # new .json file created in tests folder
        
        
    def test_extract_csv_files_from_zip_file(self):
        ""
        new_metadata_filepath = \
            epc_functions.extract_csv_files_from_zip_file(
                metadata_filepath = 'epc_tables-metadata.json',
                data_folder = '_data',
                inspection_date_start='2021-01-01',
                inspection_date_end='2021-12-31',
                verbose = False
                )
            
        self.assertEqual(
            new_metadata_filepath,
            '_data\epc_tables-metadata.json'
            )
        
        
    def test_import_table_group_to_sqlite(self):
        ""
        epc_functions.import_table_group_to_sqlite(
            metadata_filepath = '_data\epc_tables-metadata.json',
            database_filepath = '_data/epc_data.sqlite',
            verbose = False
            )
        
    
    def test_get_epc_table_names_in_database(self):
        ""
        result = epc_functions.get_epc_table_names_in_database(
            database_filepath = '_data/epc_data.sqlite',
            metadata_filepath = '_data/epc_tables-metadata.json'
            )
        
        self.assertEqual(
            result,
            [
                'domestic_certificates',
                'domestic_recommendations'
                ]
            )
    
    

class TestMainFunctions(unittest.TestCase):
    ""
    
    
    def test_get_domestic_certificates_rows(self):
        ""
        result = \
            epc_functions.get_domestic_certificates_rows(
                database_filepath = '_data/epc_data.sqlite',
                filter_by = None,  
                fields = None,  
                limit = None,
                verbose = False
                )
        #print(len(result))
        #print(result[0])
    
        self.assertEqual(
            len(result),
            2728
            )
        
        self.assertEqual(
            result[0],
            {
                'LMK_KEY': '2e01c3466f2ea4a167f0cc159a7d8766a2daa60c807ec31acc86ecb61a8d77e3', 
                'ADDRESS1': '6 ROSEWALK', 
                'ADDRESS2': 'WARFLEET', 
                'ADDRESS3': '', 
                'POSTCODE': 'TQ6 9BZ', 
                'BUILDING_REFERENCE_NUMBER': '10001567772', 
                'CURRENT_ENERGY_RATING': 'C', 
                'POTENTIAL_ENERGY_RATING': 'B', 
                'CURRENT_ENERGY_EFFICIENCY': 73, 
                'POTENTIAL_ENERGY_EFFICIENCY': 82, 
                'PROPERTY_TYPE': 'House', 
                'BUILT_FORM': 'Detached', 
                'INSPECTION_DATE': '2021-07-09', 
                'LOCAL_AUTHORITY': 'E07000044', 
                'CONSTITUENCY': 'E14001001', 
                'COUNTY': 'Devon', 
                'LODGEMENT_DATE': '2021-07-10', 
                'TRANSACTION_TYPE': 'marketed sale', 
                'ENVIRONMENT_IMPACT_CURRENT': 68, 
                'ENVIRONMENT_IMPACT_POTENTIAL': 77, 
                'ENERGY_CONSUMPTION_CURRENT': 147, 
                'ENERGY_CONSUMPTION_POTENTIAL': 90, 
                'CO2_EMISSIONS_CURRENT': 4.0, 
                'CO2_EMISS_CURR_PER_FLOOR_AREA': 26.0, 
                'CO2_EMISSIONS_POTENTIAL': 2.5, 
                'LIGHTING_COST_CURRENT': 99, 
                'LIGHTING_COST_POTENTIAL': 99, 
                'HEATING_COST_CURRENT': 656, 
                'HEATING_COST_POTENTIAL': 629, 
                'HOT_WATER_COST_CURRENT': 133, 
                'HOT_WATER_COST_POTENTIAL': 84, 
                'TOTAL_FLOOR_AREA': 155.0, 
                'ENERGY_TARIFF': 'Single', 
                'MAINS_GAS_FLAG': 'Y', 
                'FLOOR_LEVEL': '', 
                'FLAT_TOP_STOREY': '', 
                'FLAT_STOREY_COUNT': '', 
                'MAIN_HEATING_CONTROLS': '', 
                'MULTI_GLAZE_PROPORTION': 100, 
                'GLAZED_TYPE': 'double glazing, unknown install date', 
                'GLAZED_AREA': 'More Than Typical', 
                'EXTENSION_COUNT': 1, 
                'NUMBER_HABITABLE_ROOMS': 4, 
                'NUMBER_HEATED_ROOMS': 4, 
                'LOW_ENERGY_LIGHTING': 100, 
                'NUMBER_OPEN_FIREPLACES': 0, 
                'HOTWATER_DESCRIPTION': 'From main system', 
                'HOT_WATER_ENERGY_EFF': 'Good', 
                'HOT_WATER_ENV_EFF': 'Good', 
                'FLOOR_DESCRIPTION': 'Suspended, limited insulation (assumed)', 
                'FLOOR_ENERGY_EFF': 'N/A', 
                'FLOOR_ENV_EFF': 'N/A', 
                'WINDOWS_DESCRIPTION': 'Fully double glazed', 
                'WINDOWS_ENERGY_EFF': 'Average', 
                'WINDOWS_ENV_EFF': 'Average', 
                'WALLS_DESCRIPTION': 'Cavity wall, as built, insulated (assumed)', 
                'WALLS_ENERGY_EFF': 'Good', 
                'WALLS_ENV_EFF': 'Good', 
                'SECONDHEAT_DESCRIPTION': 'None', 
                'SHEATING_ENERGY_EFF': 'N/A', 
                'SHEATING_ENV_EFF': 'N/A', 
                'ROOF_DESCRIPTION': 'Roof room(s), insulated', 
                'ROOF_ENERGY_EFF': 'Good', 
                'ROOF_ENV_EFF': 'Good', 
                'MAINHEAT_DESCRIPTION': 'Boiler and radiators, mains gas', 
                'MAINHEAT_ENERGY_EFF': 'Good', 
                'MAINHEAT_ENV_EFF': 'Good', 
                'MAINHEATCONT_DESCRIPTION': 'Programmer, room thermostat and TRVs', 
                'MAINHEATC_ENERGY_EFF': 'Good', 
                'MAINHEATC_ENV_EFF': 'Good', 
                'LIGHTING_DESCRIPTION': 'Low energy lighting in all fixed outlets', 
                'LIGHTING_ENERGY_EFF': 'Very Good', 
                'LIGHTING_ENV_EFF': 'Very Good', 
                'MAIN_FUEL': 'mains gas (not community)', 
                'WIND_TURBINE_COUNT': 0, 
                'HEAT_LOSS_CORRIDOR': '', 
                'UNHEATED_CORRIDOR_LENGTH': '', 
                'FLOOR_HEIGHT': 2.41, 
                'PHOTO_SUPPLY': 0, 
                'SOLAR_WATER_HEATING_FLAG': 'N', 
                'MECHANICAL_VENTILATION': 'natural', 
                'ADDRESS': '6 ROSEWALK, WARFLEET', 
                'LOCAL_AUTHORITY_LABEL': 'South Hams', 
                'CONSTITUENCY_LABEL': 'Totnes', 
                'POSTTOWN': 'DARTMOUTH', 
                'CONSTRUCTION_AGE_BAND': 'England and Wales: 1996-2002', 
                'LODGEMENT_DATETIME': '2021-07-10 09:19:00', 
                'TENURE': 'Owner-occupied', 
                'FIXED_LIGHTING_OUTLETS_COUNT': 16, 
                'LOW_ENERGY_FIXED_LIGHT_COUNT': '', 
                'UPRN': 10008909978, 
                'UPRN_SOURCE': 'Energy Assessor'}
            )
    
    
    def test_get_domestic_certificates_row_count(self):
        ""
        result = \
            epc_functions.get_domestic_certificates_row_count(
                database_filepath = '_data/epc_data.sqlite',
                filter_by = None,
                group_by = 'PROPERTY_TYPE',
                verbose = False
                )
        #print(result)
        self.assertEqual(
            result,
            [
                {'PROPERTY_TYPE': 'Bungalow', 'COUNT': 459}, 
                {'PROPERTY_TYPE': 'Flat', 'COUNT': 368}, 
                {'PROPERTY_TYPE': 'House', 'COUNT': 1830}, 
                {'PROPERTY_TYPE': 'Maisonette', 'COUNT': 71}
                ]
            )
    
    
    def test_get_domestic_certificates_field_names(self):
        ""
        result = \
            epc_functions.get_domestic_certificates_field_names(
                database_filepath = '_data/epc_data.sqlite',
                verbose = False
                )
        #print(result)
        self.assertEqual(
            result,
            [
                'LMK_KEY', 
                'ADDRESS1', 
                'ADDRESS2', 
                'ADDRESS3', 
                'POSTCODE', 
                'BUILDING_REFERENCE_NUMBER', 
                'CURRENT_ENERGY_RATING', 
                'POTENTIAL_ENERGY_RATING', 
                'CURRENT_ENERGY_EFFICIENCY', 
                'POTENTIAL_ENERGY_EFFICIENCY', 
                'PROPERTY_TYPE', 
                'BUILT_FORM', 
                'INSPECTION_DATE', 
                'LOCAL_AUTHORITY', 
                'CONSTITUENCY', 
                'COUNTY', 
                'LODGEMENT_DATE', 
                'TRANSACTION_TYPE', 
                'ENVIRONMENT_IMPACT_CURRENT', 
                'ENVIRONMENT_IMPACT_POTENTIAL', 
                'ENERGY_CONSUMPTION_CURRENT', 
                'ENERGY_CONSUMPTION_POTENTIAL', 
                'CO2_EMISSIONS_CURRENT', 
                'CO2_EMISS_CURR_PER_FLOOR_AREA', 
                'CO2_EMISSIONS_POTENTIAL', 
                'LIGHTING_COST_CURRENT', 
                'LIGHTING_COST_POTENTIAL', 
                'HEATING_COST_CURRENT', 
                'HEATING_COST_POTENTIAL', 
                'HOT_WATER_COST_CURRENT', 
                'HOT_WATER_COST_POTENTIAL', 
                'TOTAL_FLOOR_AREA', 
                'ENERGY_TARIFF', 
                'MAINS_GAS_FLAG', 
                'FLOOR_LEVEL', 
                'FLAT_TOP_STOREY', 
                'FLAT_STOREY_COUNT', 
                'MAIN_HEATING_CONTROLS', 
                'MULTI_GLAZE_PROPORTION', 
                'GLAZED_TYPE', 
                'GLAZED_AREA', 
                'EXTENSION_COUNT', 
                'NUMBER_HABITABLE_ROOMS', 
                'NUMBER_HEATED_ROOMS', 
                'LOW_ENERGY_LIGHTING', 
                'NUMBER_OPEN_FIREPLACES', 
                'HOTWATER_DESCRIPTION', 
                'HOT_WATER_ENERGY_EFF', 
                'HOT_WATER_ENV_EFF', 
                'FLOOR_DESCRIPTION', 
                'FLOOR_ENERGY_EFF', 
                'FLOOR_ENV_EFF', 
                'WINDOWS_DESCRIPTION', 
                'WINDOWS_ENERGY_EFF', 
                'WINDOWS_ENV_EFF', 
                'WALLS_DESCRIPTION', 
                'WALLS_ENERGY_EFF', 
                'WALLS_ENV_EFF', 
                'SECONDHEAT_DESCRIPTION', 
                'SHEATING_ENERGY_EFF', 
                'SHEATING_ENV_EFF', 
                'ROOF_DESCRIPTION', 
                'ROOF_ENERGY_EFF', 
                'ROOF_ENV_EFF', 
                'MAINHEAT_DESCRIPTION', 
                'MAINHEAT_ENERGY_EFF', 
                'MAINHEAT_ENV_EFF', 
                'MAINHEATCONT_DESCRIPTION', 
                'MAINHEATC_ENERGY_EFF', 
                'MAINHEATC_ENV_EFF', 
                'LIGHTING_DESCRIPTION', 
                'LIGHTING_ENERGY_EFF', 
                'LIGHTING_ENV_EFF', 
                'MAIN_FUEL', 
                'WIND_TURBINE_COUNT', 
                'HEAT_LOSS_CORRIDOR', 
                'UNHEATED_CORRIDOR_LENGTH', 
                'FLOOR_HEIGHT', 
                'PHOTO_SUPPLY', 
                'SOLAR_WATER_HEATING_FLAG', 
                'MECHANICAL_VENTILATION', 
                'ADDRESS', 
                'LOCAL_AUTHORITY_LABEL', 
                'CONSTITUENCY_LABEL', 
                'POSTTOWN', 
                'CONSTRUCTION_AGE_BAND', 
                'LODGEMENT_DATETIME', 
                'TENURE', 
                'FIXED_LIGHTING_OUTLETS_COUNT', 
                'LOW_ENERGY_FIXED_LIGHT_COUNT', 
                'UPRN', 
                'UPRN_SOURCE']
            )   
        
    
    def test_get_domestic_recommendations_rows(self):
        ""
        result = \
            epc_functions.get_domestic_recommendations_rows(
                database_filepath = '_data/epc_data.sqlite',
                filter_by = None,  
                fields = None, 
                limit = None,
                verbose = False
                )
        #print(len(result))
        #print(result[0])
        
        self.assertEqual(
            len(result),
            195616
            )
        
        self.assertEqual(
            result[0],
            {
                'LMK_KEY': '18bae7887aae2e8cc7754a9148ad8428ac3e76c5d74e3922e852f8922348b9bb', 
                'IMPROVEMENT_ITEM': 1, 
                'IMPROVEMENT_SUMMARY_TEXT': '', 
                'IMPROVEMENT_DESCR_TEXT': '', 
                'IMPROVEMENT_ID': 6, 
                'IMPROVEMENT_ID_TEXT': 'Cavity wall insulation', 
                'INDICATIVE_COST': '£500 - £1,500'
                }
            )
        
        
    def test_get_domestic_recommendations_row_count(self):
        ""
        result = \
            epc_functions.get_domestic_recommendations_row_count(
                database_filepath = '_data/epc_data.sqlite',
                filter_by = None,
                group_by = 'IMPROVEMENT_ID_TEXT',
                verbose = False
                )
        #print(result)
        
        self.assertEqual(
            result,
            [
                {'IMPROVEMENT_ID_TEXT': '', 'COUNT': 15693}, 
                {'IMPROVEMENT_ID_TEXT': '50 mm internal or external wall insulation', 'COUNT': 9217}, 
                {'IMPROVEMENT_ID_TEXT': 'Add additional 80 mm jacket to hot water cylinder', 'COUNT': 1960}, 
                {'IMPROVEMENT_ID_TEXT': 'Cavity wall insulation', 'COUNT': 7850}, 
                {'IMPROVEMENT_ID_TEXT': 'Change heating to gas condensing boiler', 'COUNT': 505}, 
                {'IMPROVEMENT_ID_TEXT': 'Change room heaters to condensing boiler', 'COUNT': 277}, 
                {'IMPROVEMENT_ID_TEXT': 'Condensing oil boiler with radiators', 'COUNT': 10}, 
                {'IMPROVEMENT_ID_TEXT': 'Draughtproof single-glazed windows', 'COUNT': 2544}, 
                {'IMPROVEMENT_ID_TEXT': 'Fan assisted storage heaters', 'COUNT': 1236}, 
                {'IMPROVEMENT_ID_TEXT': 'Fan assisted storage heaters and dual immersion cylinder', 'COUNT': 1111}, 
                {'IMPROVEMENT_ID_TEXT': 'Fan-assisted storage heaters', 'COUNT': 185}, 
                {'IMPROVEMENT_ID_TEXT': 'Flat roof insulation', 'COUNT': 1830}, 
                {'IMPROVEMENT_ID_TEXT': 'Floor insulation', 'COUNT': 6967}, 
                {'IMPROVEMENT_ID_TEXT': 'Flue gas heat recovery device in conjunction with boiler', 'COUNT': 505}, 
                {'IMPROVEMENT_ID_TEXT': 'Heat recovery system for mixer showers', 'COUNT': 504}, 
                {'IMPROVEMENT_ID_TEXT': 'High heat retention storage heaters', 'COUNT': 2138}, 
                {'IMPROVEMENT_ID_TEXT': 'High heat retention storage heaters and dual immersion cylinder', 'COUNT': 1701}, 
                {'IMPROVEMENT_ID_TEXT': 'High performance external doors', 'COUNT': 1841}, 
                {'IMPROVEMENT_ID_TEXT': 'Hot water cylinder thermostat', 'COUNT': 1331}, 
                {'IMPROVEMENT_ID_TEXT': 'Increase hot water cylinder insulation', 'COUNT': 2370}, 
                {'IMPROVEMENT_ID_TEXT': 'Increase loft insulation to 270 mm', 'COUNT': 7861}, 
                {'IMPROVEMENT_ID_TEXT': 'Install condensing boiler', 'COUNT': 418}, 
                {'IMPROVEMENT_ID_TEXT': 'Insulate hot water cylinder with 80 mm jacket', 'COUNT': 345}, 
                {'IMPROVEMENT_ID_TEXT': 'Low energy lighting for all fixed outlets', 'COUNT': 20892}, 
                {'IMPROVEMENT_ID_TEXT': 'Party wall insulation', 'COUNT': 639}, 
                {'IMPROVEMENT_ID_TEXT': 'Replace boiler with biomass boiler', 'COUNT': 18}, 
                {'IMPROVEMENT_ID_TEXT': 'Replace boiler with new condensing boiler', 'COUNT': 9283}, 
                {'IMPROVEMENT_ID_TEXT': 'Replace heating unit with condensing unit', 'COUNT': 5}, 
                {'IMPROVEMENT_ID_TEXT': 'Replace single glazed windows with low-E double glazing', 'COUNT': 4412}, 
                {'IMPROVEMENT_ID_TEXT': 'Replacement glazing units', 'COUNT': 1056}, 
                {'IMPROVEMENT_ID_TEXT': 'Replacement warm air unit', 'COUNT': 40}, 
                {'IMPROVEMENT_ID_TEXT': 'Room-in-roof insulation', 'COUNT': 1626}, 
                {'IMPROVEMENT_ID_TEXT': 'Secondary glazing to single glazed windows', 'COUNT': 154}, 
                {'IMPROVEMENT_ID_TEXT': 'Solar photovoltaic panels, 2.5 kWp', 'COUNT': 29755}, 
                {'IMPROVEMENT_ID_TEXT': 'Solar water heating', 'COUNT': 28097}, 
                {'IMPROVEMENT_ID_TEXT': 'Solid floor insulation', 'COUNT': 9026}, 
                {'IMPROVEMENT_ID_TEXT': 'Suspended floor insulation', 'COUNT': 5748}, 
                {'IMPROVEMENT_ID_TEXT': 'Time and temperature zone control', 'COUNT': 408}, 
                {'IMPROVEMENT_ID_TEXT': 'Upgrade heating controls', 'COUNT': 6048}, 
                {'IMPROVEMENT_ID_TEXT': 'Upgrading heating controls', 'COUNT': 1209}, 
                {'IMPROVEMENT_ID_TEXT': 'Wind turbine', 'COUNT': 8768}, 
                {'IMPROVEMENT_ID_TEXT': 'Wood pellet stove with boiler and radiators', 'COUNT': 33}
                ]
            )
        
        
    def test_get_domestic_recommendations_field_names(self):
        ""
        result = \
            epc_functions.get_domestic_recommendations_field_names(
                database_filepath = '_data/epc_data.sqlite',
                verbose = False
                )
        #print(result)
        
        self.assertEqual(
            result,
            [
                'LMK_KEY', 
                'IMPROVEMENT_ITEM', 
                'IMPROVEMENT_SUMMARY_TEXT', 
                'IMPROVEMENT_DESCR_TEXT', 
                'IMPROVEMENT_ID', 
                'IMPROVEMENT_ID_TEXT', 
                'INDICATIVE_COST'
                ]
            )
        
        
        

if __name__ == '__main__':
    
    unittest.main()
