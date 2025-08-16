#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   faker_expdate.py
#
#   Description     :   Create a dataset representing a demographic distribution
#
#   Created     	:   06 Aug 2025
#
#                   :   Custom Faker Provider for generating DD/YY format dates
#
#   Functions       :   DateMMYYProvider (Class)
#                   :       mm_yy
#                   :       exp_date
#
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.1"
__copyright__   = "Copyright 2025, - George Leonard"


import random
from faker.providers import BaseProvider
from datetime import datetime

class DateMMYYProvider(BaseProvider):

    """
    Custom Faker Provider for generating random dates in MM/YY format
    within a specified year range from a start year.
    """
    
    def mm_yy(self, start_year=None, year_range=5):
        
        """
        Generate a random date in MM/YY format within a specified range from start year.
        
        Args:
            start_year (int, optional): Starting year (2-digit or 4-digit). 
                                      Defaults to current year - 10
            year_range (int): Number of years from start year to include in range. 
                            Defaults to 3
        
        Returns:
            str: Random date string in MM/YY format
            
        Examples:
            fake.mm_yy(2020, 5)  # Random date between 2020-2024 -> "03/22"
            fake.mm_yy(90, 10)   # Random date between 1990-1999 -> "11/95"
            fake.mm_yy()         # Uses default range from 10 years ago
        """
        
        # Set default start year if not provided
        if start_year is None:
            start_year = datetime.now().year
        

        # Handle 2-digit and 4-digit years
        if start_year < 100:
            # For 2-digit years, assume 20xx for 00-25 , 19xx for 26-99
            if start_year <= 25:
                full_start_year = 2000 + start_year
                
            else:
                full_start_year = 1900 + start_year
                
        else:
            full_start_year = start_year
        
        # Calculate end year
        end_year = full_start_year + year_range
        
        # Generate random year within range
        random_year = self.random_int(full_start_year, end_year)

        # Safetycheck as we've had funnies, if we some how got a card generated with a exp in the pass lets fix it.
        if random_year < datetime.now().year:
            random_year  = datetime.now().year
            random_month = self.random_int(datetime.now().month+1, 12)  
            
        else:
            # Just to make sure if the card expire this year that it is still to be in coming months.
            if random_year == datetime.now().year:
                random_month = self.random_int(datetime.now().month+1, 12)  
                
            else:
                # Generate random month (1-12)
                random_month = self.random_int(1, 12)
        
        # Convert year to 2-digit format
        yy = random_year % 100

        # Format with leading zeros
        return f"{random_month:02d}/{yy:02d}"
    #end mm_yy
    
    
    def exp_date(self, year_range=3):
        
        """
        Generate a random date in MM/YY format from the past.
        
        Args:
            year_range (int): Number of years in the past to consider
            We don't pass this through so that we can default to the defaault value for mm_yy which is more than year range to allow any card to have life left.
        
        Returns:
            str: Random past date in MM/YY format
        """

        current_year = datetime.now().year
        start_year   = current_year - self.random_int(1, year_range)
        range        = self.random_int(year_range, 5)

        return self.mm_yy(
                start_year, 
                range
            )
    #end exp_date
#end DateMMYYProvider
