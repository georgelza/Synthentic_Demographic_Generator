#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   getAccount.py
#
#   Description     :   
#
#   Created     	:   06 Aug 2025
#
#                   :   'mastercard', 'visa', 'amex', 'jcb', and 'diners'
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.1"
__copyright__   = "Copyright 2025, - George Leonard"


from utils import *
from weighted_random import *
from option_lists import *
from faker_bank import *


def createBankAccount(fake, init, surname):
    
    arAccounts        = []
    x                 = 0
    NumberOfAccounts  = WeightedRandomSelector(bank_accounts_per_person, scale=1).get_random()
    
    while x < NumberOfAccounts:
        
        bank_name     = WeightedRandomSelector(banks_options,  scale=1.33).get_random()            
        bank_record   = fake.get_bank_info(bank_name)
        genAccount    = fake.unique.irish_iban_account_number(bank_record["iban_structure"])

        account = {
            "bank":             bank_record["name"],
            "bicfi_code":       bank_record["bicfi_code"],
            "swift_code":       bank_record["swift_code"],
            "iban_structure":   bank_record["iban_structure"],          # Basically our unique bank id/reference
            "accountNumber":    genAccount,
            "accountType":      WeightedRandomSelector(accountTypes_options, scale=1).get_random()
        }
        arAccounts.append(account)
        x += 1

    return createCCAccount(fake, init, surname, arAccounts)
#end createBankAccount


def createCCAccount(fake, init, surname, arAccounts):
    
    x           = 0
    NumberOfCC  = WeightedRandomSelector(credit_cards_per_person, scale=1).get_random()
    
    while x < NumberOfCC:
        
        bank_name     = WeightedRandomSelector(banks_options,  scale=1.33).get_random()    
        bank_record   = fake.get_bank_info(bank_name)
        
        card_network_options = bank_record["card_network"]
        cardNetwork          = WeightedRandomSelector(card_network_options, scale=1).get_random()
        
        if cardNetwork == "Mastercard":    # Generate a Mastercard number
            ccNum = fake.unique.credit_card_number(card_type='mastercard')

        elif cardNetwork == "Visa":         # Generate a Visa number
            ccNum = fake.unique.credit_card_number(card_type='visa')

        elif cardNetwork == "Amex":         # Generate an American Express number
            ccNum = fake.unique.credit_card_number(card_type='amex')

        elif cardNetwork == "Jcb":         # Generate an American Express number
            ccNum = fake.unique.credit_card_number(card_type='jcb')
        
        elif cardNetwork == "Diners":         # Generate an American Express number
            ccNum = fake.unique.credit_card_number(card_type='diners')
        
        card = {
            "card_holder":      f"{init} {surname}",
            "card_number":      ccNum,
            "exp_date":         fake.exp_date(),
            "card_network":     cardNetwork,
            "issuing_bank":     bank_name,
            "iban_structure":   bank_record["iban_structure"],          # Basically our unique bank id/reference
        }    
    
        arAccounts.append(card)
        x += 1

    return arAccounts
#end createBankAccount




# # Example usage and test function
# def test_bank_extractor():
#     """
#     Test function to demonstrate usage
#     """
    
#     # Example JSON data (you would load this from your file)
#     sample_json = '''[
#         {
#             "id": "IE-AIB-001",
#             "name": "Allied Irish Banks, p.l.c.",
#             "country": "Ireland",
#             "swift_code": "AIBKIE2D"
#         }
#     ]'''
    
#     # Test cases
#     test_cases = [
#         "Allied Irish Banks",
#         "AIB", 
#         "allied",
#         "Bank of Ireland",
#         "BOI",
#         "Permanent TSB",
#         "PTSB",
#         "Citibank",
#         "Barclays",
#         "Bank of America"
#     ]
    
#     print("Testing bank extraction:")
#     for test_name in test_cases:
#         result = get_bank_info(test_name, sample_json)
#         if result:
#             print(f"✓ Found: {test_name} -> {result['name']}")
#         else:
#             print(f"✗ Not found: {test_name}")

# if __name__ == "__main__":
#     # Load your actual JSON file
#     with open('ie_banks.json', 'r') as file:
#         banks_data = json.load(file)
    
#     # Example usage:
#     bank = get_bank_info("AIB", banks_data)
#     if bank:
#         print(f"Bank: {bank['name']}")
#         print(f"SWIFT: {bank['swift_code']}")
#         print(f"IBAN: {bank['iban_structure']}")
    
#     # Get all bank names
#     all_banks = get_all_bank_names(banks_data)
#     print(f"Available banks: {all_banks}")