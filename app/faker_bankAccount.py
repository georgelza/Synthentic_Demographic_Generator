#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   faker_bankAccount.py
#
#   Description     :   Create a dataset representing a demographic distribution
#
#   Created     	:   06 Aug 2025
#
#   Functions       :   IrishBankAccountProvider (Class)
#                   :       irish_iban_account_number
#
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.1"
__copyright__   = "Copyright 2025, - George Leonard"


from faker.providers.python import Provider as PythonProvider   # Import Provider from python for numerify
from faker.providers.bank   import Provider as BankProvider       # Can also use bank provider methods if needed


class IrishBankAccountProvider(PythonProvider):
    
    """
    A custom Faker provider for generating Irish bank account numbers
    by combining a given IBAN prefix with a random 8-digit sequence.
    """
    
    def irish_iban_account_number(self, iban_prefix: str) -> str:
        
        """
        Generates an Irish IBAN-like bank account number.

        Args:
            iban_prefix (str): The initial part of the IBAN (Country Code + Check Digits + Bank Identifier + Sort Code).
                               Example: "IE29AIBK931152"

        Returns:
            str: A string combining the IBAN prefix with a random 8-digit account number.
        """
        
        # Generate a random 8-digit number
        # '########' ensures exactly 8 digits are generated
        random_account_suffix = self.numerify('########')
        
        # Combine the IBAN prefix with the random 8-digit suffix
        full_iban_like_number = f"{iban_prefix}{random_account_suffix}"
        
        return full_iban_like_number
        #end irish_iban_account_number
#end IrishBankAccountProvider


# # --- Demonstration ---

# # Initialize Faker
# fake = Faker('en_IE') # Using 'en_IE' locale for potentially relevant data if other providers were used

# # Add the custom provider to the Faker instance
# fake.add_provider(IrishBankAccountProvider)

# print("Generated Irish Bank Account Numbers:")

# # Example for Allied Irish Banks (AIB)
# aib_iban_prefix = "IE29AIBK931152"
# for _ in range(3):
#     print(f"AIB (IBAN prefix: {aib_iban_prefix}): {fake.irish_iban_account_number(aib_iban_prefix)}")

# print("\n")

# # Example for Bank of Ireland (BOI)
# boi_iban_prefix = "IE79BOFI905838"
# for _ in range(3):
#     print(f"Bank of Ireland (IBAN prefix: {boi_iban_prefix}): {fake.irish_iban_account_number(boi_iban_prefix)}")

# print("\n")

# # Example for Permanent TSB (PTSB)
# ptsb_iban_prefix = "IE55IPBS990602"
# for _ in range(3):
#     print(f"Permanent TSB (IBAN prefix: {ptsb_iban_prefix}): {fake.irish_iban_account_number(ptsb_iban_prefix)}")