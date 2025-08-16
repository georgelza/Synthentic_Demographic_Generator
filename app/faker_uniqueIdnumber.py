#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   faker_uniqueIdnumber.py
#
#   Description     :   Create a Faker provider class, c
#                   :       Creating a SA ID Number, male or female for a specified input date.
#                   :       Creating a Irish PPS Number
#                   
#                   :   TO ADD MORE LOCALE SPECIFIC National Identification numbers do a copy paste of the below and add 
#                   :   relevant logic, then go to main.py line 86 and add you class, then go to getIDNumber and add a elif 
#                   :   config_params["LOCALE"] == "en_IE" with yor country code and do the call. You can now use the generic 
#                   :   generate_IdNumbers function call.
#
#   Created     	:   06 Aug 2025
#
#   Functions       :   IrishPpsNumberProvider
#                   :       pps_number
#                   :   SAIdNumberProvider
#                   :       sa_id_number
#                   :       _calculate_luhn_check_digit
#                   :       validate_sa_id
#                   
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.1"
__copyright__   = "Copyright 2025, - George Leonard"


from faker.providers import BaseProvider
import random
from datetime import datetime


class IrishPpsNumberProvider(BaseProvider):
    
    """
    A Faker provider for generating valid Irish Personal Public Service (PPS) numbers.
    """
    
    def pps_number(self):
        
        """
        Generates a valid Irish Personal Public Service (PPS) number.

        A PPS number consists of seven digits followed by one or two letters.
        The letters are determined by a checksum algorithm based on the digits.
        """
        # Generate 7 random digits for the numerical part of the PPS number
        # We ensure these are actual digits (0-9)
        digits = [random.randint(0, 9) for _ in range(7)]

        # Calculate the checksum letter(s)
        # The weights for the first 7 digits are 8, 7, 6, 5, 4, 3, 2 respectively.
        checksum_sum = sum(digits[i] * (8 - i) for i in range(7))

        # The checksum algorithm maps remainders to specific letters.
        # This custom alphabet ensures the correct mapping for modulo 23.
        # 'W' corresponds to 0, 'A' to 1, 'B' to 2, ..., 'V' to 22.
        # This is a specific sequence to match the official algorithm.
        checksum_alphabet = "WABCDEFGHIJKLMNOPQRSTUV"

        # Calculate the remainder when the sum is divided by 23
        remainder = checksum_sum % 23

        # Get the checksum letter using the remainder as an index
        checksum_letter = checksum_alphabet[remainder]

        # Format the PPS number as a string
        pps_number_str = "".join(map(str, digits)) + checksum_letter

        return pps_number_str
    #end pps_number
#end IrishPpsNumberProvider


class SAIdNumberProvider(BaseProvider):
    
    """
    South African ID Number provider for Faker.
    
    SA ID format: YYMMDDGSSSCAZ
    - YYMMDD: Date of birth
    - G: Gender (0-4 female, 5-9 male)  
    - SSS: Sequence number (000-999)
    - C: Citizenship (0 = SA citizen, 1 = permanent resident)
    - A: Usually 8 or 9 (race classifier, now obsolete but still used)
    - Z: Check digit (calculated using Luhn algorithm)
    """
    
    def sa_id_number(self, birth_date=None, gender=None, citizen=True):
        
        """
        Generate a South African ID number.
        
        Args:
            birth_date (str or datetime): Birth date in 'YY/MM/DD', 'YYYY/MM/DD', or datetime object
            gender (str): 'male', 'female', 'M', 'F' (case insensitive)
            citizen (bool): True for SA citizen (0), False for permanent resident (1)
        
        Returns:
            str: 13-digit South African ID number
        """
        
        # Handle birth date
        if birth_date is None:
            # Use faker's random for consistency with unique functionality
            birth_date = self.generator.date_of_birth(minimum_age=18, maximum_age=80)
            
        elif isinstance(birth_date, str):
            # Parse string date
            if len(birth_date.split('/')[0]) == 2:  # YY/MM/DD format
                birth_date = datetime.strptime(birth_date, '%y/%m/%d')
                
            else:  # YYYY/MM/DD format
                birth_date = datetime.strptime(birth_date, '%Y/%m/%d')
        
        # Format date part (YYMMDD)
        date_part = birth_date.strftime('%y%m%d')
        
        # Handle gender digit (G) - use faker's random for uniqueness support
        if gender is None:
            gender_digit = self.generator.random.randint(0, 9)
        else:
            gender_lower = str(gender).lower()
            if gender_lower in ['female', 'f']:
                gender_digit = self.generator.random.randint(0, 4)  # 0-4 for female
                
            elif gender_lower in ['male', 'm']:
                gender_digit = self.generator.random.randint(5, 9)  # 5-9 for male
                
            else:
                gender_digit = self.generator.random.randint(0, 9)  # Random if invalid input
        
        # Sequence number (SSS) - use faker's random for uniqueness support
        sequence = self.generator.random.randint(0, 999)
        sequence_part = f"{sequence:03d}"
        
        # Citizenship digit (C)
        citizenship_digit = 0 if citizen else 1
        
        # Race classifier (A) - use faker's random for uniqueness support
        race_digit = self.generator.random.choice([8, 9])
        
        # Construct ID without check digit
        id_without_check = f"{date_part}{gender_digit}{sequence_part}{citizenship_digit}{race_digit}"
        
        # Calculate check digit using Luhn algorithm
        check_digit = self._calculate_luhn_check_digit(id_without_check)
        
        # Return complete ID
        return f"{id_without_check}{check_digit}"
    #end sa_id_number
    
    
    def _calculate_luhn_check_digit(self, id_number):
        
        """
        Calculate Luhn check digit for SA ID number.
        
        Args:
            id_number (str): 12-digit ID number without check digit
            
        Returns:
            int: Check digit (0-9)
        """
        # Convert to list of integers
        digits = [int(d) for d in id_number]
        
        # Apply Luhn algorithm
        total = 0
        for i, digit in enumerate(reversed(digits)):
            if i % 2 == 0:  # Every second digit from right
                doubled = digit * 2
                total += doubled if doubled < 10 else doubled - 9
            else:
                total += digit
        
        # Check digit makes total divisible by 10
        return (10 - (total % 10)) % 10
    #end _calculate_luhn_check_digit
    
    
    def validate_sa_id(self, id_number):
        """
        Validate a South African ID number.
        
        Args:
            id_number (str): 13-digit SA ID number
            
        Returns:
            bool: True if valid, False otherwise
        """
        if len(id_number) != 13 or not id_number.isdigit():
            return False
        
        # Extract components
        date_part         = id_number[:6]
        gender_digit      = int(id_number[6])
        citizenship_digit = int(id_number[9])
        race_digit        = int(id_number[10])
        check_digit       = int(id_number[12])
        
        # Validate date
        try:
            datetime.strptime(date_part, '%y%m%d')
        except ValueError:
            return False
        
        # Validate other components
        if citizenship_digit not in [0, 1]:
            return False
        
        if race_digit not in [8, 9]:
            return False
        
        # Validate check digit
        expected_check = self._calculate_luhn_check_digit(id_number[:12])
        return check_digit == expected_check
    #end validate_sa_id
#end class SAIDProvider


# Example usage for Irish PPS Number
# if __name__ == "__main__":
#     # Initialize Faker
#     fake = Faker('en_IE') # Use 'en_IE' locale if you want other Irish specific data

#     # Add the custom provider to Faker
#     fake.add_provider(IrishPpsNumberProvider)

#     print("--- Generating 5 random PPS Numbers ---")
#     for _ in range(5):
#         print(fake.pps_number())

#     print("\n--- Generating 5 unique PPS Numbers ---")
#     # Faker's unique() method ensures the generated values are distinct
#     # The 'unique' attribute tracks generated values within the current Faker instance.
#     for _ in range(5):
#         print(fake.unique.pps_number())

#     # Note: For large numbers of unique values, the uniqueness tracker
#     # might eventually run out of possible combinations for the given method.
#     # In such cases, you might need to reset the unique tracker or consider
#     # the limitations of the data you are trying to generate uniquely.


# Example usage and testing for SA Idenumber
# if __name__ == "__main__":
#     from faker import Faker
    
#     # Create faker instance and add the provider
#     fake = Faker()
#     fake.add_provider(SAIDProvider)
    
#     print("Testing SA ID Provider:")
#     print("=" * 30)
    
#     # Test basic functionality
#     print("Basic ID generation:")
#     for i in range(3):
#         print(f"  {fake.sa_id_number()}")
    
#     print("\nSpecific parameters:")
#     print(f"  Male 74/04/10: {fake.sa_id_number(birth_date='74/04/10', gender='male')}")
#     print(f"  Female 92/12/27: {fake.sa_id_number(birth_date='92/12/27', gender='F')}")
    
#     print("\nUnique generation test:")
#     fake_unique = Faker()
#     fake_unique.add_provider(SAIDProvider)
    
#     unique_ids = []
#     for i in range(5):
#         unique_id = fake_unique.unique.sa_id_number(birth_date='74/04/10', gender='male')
#         unique_ids.append(unique_id)
#         print(f"  {unique_id}")
    
#     print(f"All unique: {len(set(unique_ids)) == len(unique_ids)}")