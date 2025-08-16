#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator. - Read & parse bank data file.
#
#   File            :   faker_bank.py
#
#   Description     :   Load/Retrieve specified bank data from banks_data dictionary
#
#   Created     	:   06 Aug 2025
#
#   Functions       :   BankProvider
#                   :       __init__
#                   :       _load_banks_data
#                   :       get_bank_info
#                   :       bank_name
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.2"
__copyright__   = "Copyright 2025, - George Leonard"


import json, sys
from faker.providers import BaseProvider
from typing import Dict, List, Optional, Union

class BankProvider(BaseProvider):
    
    def __init__(self, generator, file_path, mylogger):
        super().__init__(generator)
        self.mylogger   = mylogger
        self.banks_data = self._load_banks_data(file_path)
    #end __init__


    def _load_banks_data(self, file_path: str) -> List[Dict]:
        
        """
        Loads bank data from a JSON file.
        """

        try:
            with open(file_path, 'r') as file:

                json_file = json.load(file)

                self.mylogger.info("Successfully loaded bank data from {file_path}".format(
                    file_path = file_path
                ))

                return json_file
            #end with
    
        except FileNotFoundError as err:
            self.mylogger.err("File Not Found error: {file} {err}".format(
                file = file_path,
                err  = err
            ))
            return []

        except IOError as err:
            self.mylogger.err("An error occurred while reading the file: {file} {err}".format(
                file = file_path,
                err  = err
            ))
            return []
            
        except json.JSONDecodeError as err:
            self.mylogger.err("Could not decode JSON from: {file} {err}".format(
                file = file_path,
                err  = err
            ))
            return []
        
        except Exception as err:        
            self.mylogger.err("Generic File Read error: {file} {err}".format(
                file = file_path,
                err  = err
            ))

            return []
        #end try
    #end _load_banks_data
    

    def get_bank_info(self, bank_name: str) -> Optional[Dict]:
        
        """
        Extracts bank information based on bank name.
        """
        
        search_name = bank_name.lower().strip()
        for bank in self.banks_data:
            bank_full_name = bank.get('name', '').lower()
            if (search_name == bank_full_name or 
                search_name in bank_full_name or
                any(search_name in word for word in bank_full_name.split())):
                
                return bank
            #end if
        #end for
        return None
    #end get_bank_info
    
    
    def bank_name(self) -> str:
        
        """
        Returns a random bank name from the loaded data.
        """
        
        if self.banks_data:
            return self.random_element(self.banks_data)['name']
        return ""
    #end bank_name
#end BankProvider