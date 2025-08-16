#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   faker_address_provider.py
#
#   Description     :   OOP-based Address Provider for Faker - Create addresses representing a demographic distribution
#
#   Created     	:   16 Aug 2025 (Refactored from faker_address.py)
#
#   Classes         :   GeographicDataProvider - Main provider class for Faker
#
#   Methods         :   __init__ - Initialize with data file path
#                   :   load_data - Load JSON data from file
#                   :   generate_address - Generate complete address
#                   :   get_provinces - Get provinces data with population values
#                   :   get_counties - Get counties for a specific province
#                   :   get_cities_towns - Get cities/towns for a specific province and county
#
#
# ########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.2"
__copyright__   = "Copyright 2025, - George Leonard"
    
import json
from faker.providers import BaseProvider


class GeographicDataProvider(BaseProvider):
    
    """
    A Faker provider for generating realistic addresses based on geographic demographic data.
    
    This provider loads census/demographic data from a JSON file and uses population weights
    to generate realistic addresses that reflect actual population distributions.
    """
    
    def __init__(self, generator, file_path=None, mylogger=None):
        
        """
        Initialize the Geographic Data Provider.
        
        Args:
            generator:       The Faker generator instance
            file_path (str): Path to the JSON data file containing geographic data
            mylogger:        Logger instance for logging operations
        """
        
        super().__init__(generator)
        self.mylogger                   = mylogger
        self.data                       = None
        self.provinces_cache            = None
        self.total_province_population  = 0
        
        if file_path:
            self.load_data(file_path)
        #end if
    #end __init__
    
    
    def load_data(self, file_path):
        
        """
        Load geographic data from JSON file.
        
        Args:
            file_path (str): Path to the JSON data file
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                self.data = json.load(file)
            #end with
                
            # Cache provinces data for efficiency
            self.provinces_cache, self.total_province_population = self._extract_provinces()
            
            if self.mylogger:
                self.mylogger.info("Successfully loaded geographic data from {file_path}".format(
                    file_path = file_path
                ))
                
                self.mylogger.info("Loaded data for {country} with {provinces} provinces".format(
                    country   = self.data.get('country', 'Unknown'),
                    provinces = len(self.provinces_cache)
                ))
            #end if
                
        except FileNotFoundError as err:
            error_msg = f"Geographic data file not found: {file_path}"
            
            if self.mylogger:
                self.mylogger.error(error_msg)
            
            #end if
            raise FileNotFoundError(error_msg) from err
            
        except json.JSONDecodeError as err:
            error_msg = f"Invalid JSON in geographic data file: {file_path}"
            
            if self.mylogger:
                self.mylogger.error(error_msg)
            #end if
            
            raise json.JSONDecodeError(error_msg) from err
        #end try
    #end load_data
    
    def _extract_provinces(self):
        
        """
        Extract provinces data with total population sum.
        
        Returns:
            tuple: (provinces_list, total_population)
        """
        
        if not self.data or 'provinces' not in self.data:
            return [], 0
        #end if
            
        provinces        = []
        total_population = 0
        
        for province in self.data['provinces']:
            province_data = {
                'name':  province['name'],
                'value': province.get('population', 0)  # Use 'population' instead of 'value'
            }
            
            provinces.append(province_data)
            total_population += province_data['value']
        #end for
        
        return provinces, total_population
    #end _extract_provinces
    
    
    def generate_address(self, town=None, county=None, province_state=None, country=None):
        
        """
        Generate a complete address with realistic demographic distribution.
        
        Args:
            town (str, optional):            Specific town/city to use
            county (str, optional):          Specific county to use
            province_state (str, optional):  Specific province/state to use
            country (str, optional):         Specific country to use
            
        Returns:
            dict: Complete address with street, town, county, state, post_code, country
        """
        
        if not self.data:
            raise ValueError("No geographic data loaded. Call load_data() first.")
        #end if
        
        # Use provided country or get from data
        final_country = country or self.data.get('country', 'Unknown')
        
        # Generate street components
        street_number = self.generator.building_number()
        street_name   = self.generator.street_name()
        street_suffix = self.generator.street_suffix()  # Ave, St, Blvd, etc.
        post_code     = self.generator.postcode()
        
        return {
            'street':       f"{street_number} {street_name} {street_suffix}",
            'town':         town or 'Unknown Town',
            'county':       county or 'Unknown County',
            'state':        province_state or 'Unknown State',
            'post_code':    post_code,
            'country':      final_country
        }
    #end generate_address
    
    
    def get_provinces(self):

        """
        Get provinces data with total population sum.
        
        Returns:
            tuple: (provinces_list, total_population)
            
        Raises:
            ValueError: If no data is loaded
        """

        if not self.data:
            raise ValueError("No geographic data loaded. Call load_data() first.")
        #end if
                    
        return self.provinces_cache.copy(), self.total_province_population
    #end get_provinces
    
    
    def get_counties(self, province_name):
        
        """
        Get counties data for a specific province with total population sum.
        
        Args:
            province_name (str): Name of the province (e.g., "Leinster")
            
        Returns:
            tuple: (counties_list, total_population) or (None, None) if province not found
            
        Raises:
            ValueError: If no data is loaded
        """
        
        if not self.data:
            raise ValueError("No geographic data loaded. Call load_data() first.")
        #end if
        
        counties         = []
        total_population = 0
        
        # Find the specified province
        target_province = None
        for province in self.data['provinces']:
            if province['name'].lower() == province_name.lower():
                target_province = province
                
                break
            #enf if
        #end for
        
        if not target_province:
            if self.mylogger:
                self.mylogger.warning(f"Province '{province_name}' not found in data".format(
                    province_name = province_name
                ))
            #end if
            return None, None
        #end if
        
        # Extract counties data
        if 'counties' in target_province:
            for county in target_province['counties']:
                county_data = {
                    'name':  county['name'],
                    'value': county.get('population', 0)
                }
                 
                counties.append(county_data)
                total_population += county_data['value']
            #end for
        #end if
        return counties, total_population
    #end get_counties
    
    
    def get_cities_towns(self, province_name, county_name):
        
        """
        Get cities/towns data for a specific province and county with total population sum.
        
        Args:
            province_name (str): Name of the province (e.g., "Leinster")
            county_name (str):   Name of the county (e.g., "Dublin")
            
        Returns:
            tuple: (cities_towns_list, total_population) or (None, None) if not found
            
        Raises:
            ValueError: If no data is loaded
        """
        
        if not self.data:
            raise ValueError("No geographic data loaded. Call load_data() first.")
        #end if
        
        cities_towns     = []
        total_population = 0
        
        # Find the specified province
        target_province = None
        for province in self.data['provinces']:
            if province['name'].lower() == province_name.lower():
                target_province = province
                break
            #end if
        #end for
        
        if not target_province:
            if self.mylogger:
                self.mylogger.warning("Province '{province_name}' not found in data".format(
                    province_name = province_name
                ))
            #end if
            return None, None
        #end if
        
        
        # Find the specified county
        target_county = None
        for county in target_province.get('counties', []):
            if county['name'].lower() == county_name.lower():
                target_county = county
                break
            #end if
        #end for
        
        
        if not target_county:
            if self.mylogger:
                self.mylogger.warning("County '{county_name}' not found in province '{province_name}'".format(
                    county_name   = county_name,
                    province_name = province_name
                ))
            #end if
            return None, None
        #end if
        
        
        # Extract cities/towns data
        if 'cities_towns' in target_county:
            for city_town in target_county['cities_towns']:
                city_town_data = {
                    'name':  city_town['name'],
                    'value': city_town.get('population', 0)
                }
                                
                cities_towns.append(city_town_data)
                total_population += city_town_data['value']
            #end for
        #end if
        
        return cities_towns, total_population
    #end get_cities_towns
    
    
    def get_country_info(self):
        
        """
        Get basic country information from loaded data.
        
        Returns:
            dict: Country information including name, population, census_year, etc.
            
        Raises:
            ValueError: If no data is loaded
        """
        
        if not self.data:
            raise ValueError("No geographic data loaded. Call load_data() first.")
        #end if
        
        return {
            'country':                      self.data.get('country'),
            'population':                   self.data.get('population'),
            'census_year':                  self.data.get('census_year'),
            'national_average_age':         self.data.get('national_average_age'),
            'national_male_population':     self.data.get('national_male_population'),
            'national_female_population':   self.data.get('national_female_population')
        }
    #end get_country_info
#end Class


# Convenience functions to maintain backward compatibility with existing code
def genAddress(fake, town, county, provinces_state, country):
    
    """
    Backward compatibility function for existing code.
    
    Args:
        fake:                   Faker instance (should have GeographicDataProvider added)
        town (str):             Town name
        county (str):           County name  
        provinces_state (str):  Province/state name
        country (str):          Country name
        
    Returns:
        dict: Address dictionary
    """
    
    if hasattr(fake, 'generate_address'):
        return fake.generate_address(town, county, provinces_state, country)
    
    else:
        # Fallback to original logic if provider not properly registered
        street_number   = fake.building_number()
        street_name     = fake.street_name()
        street_suffix   = fake.street_suffix()
        post_code       = fake.postcode()
        
        return {
            'street':       f"{street_number} {street_name} {street_suffix}",
            'town':         town,
            'county':       county,
            'state':        provinces_state,
            'post_code':    post_code,
            'country':      country
        }
    #end if
#end genAddress


def get_provinces(data):
    
    """
    Backward compatibility function for existing code.
    
    Args:
        data (dict): Raw JSON data
        
    Returns:
        tuple: (provinces_list, total_population)
    """
    
    provinces        = []
    total_population = 0
    
    for province in data.get('provinces', []):
        province_data = {
            'name':  province['name'],
            'value': province.get('population', province.get('value', 0))
        }

        provinces.append(province_data)
        total_population += province_data['value']

    #end for
    return provinces, total_population
#end get_provinces

def get_counties(data, province_name):
    
    """
    Backward compatibility function for existing code.
    
    Args:
        data (dict):         Raw JSON data
        province_name (str): Name of the province
        
    Returns:
        tuple: (counties_list, total_population) or (None, None)
    """
    
    counties         = []
    total_population = 0
    
    # Find the specified province
    target_province = None
    for province in data.get('provinces', []):
        if province['name'].lower() == province_name.lower():
            target_province = province
            
            break
        #end if
    #end for
    
    if not target_province:
        return None, None
    #end if
    
    # Extract counties data
    for county in target_province.get('counties', []):
        county_data = {
            'name':  county['name'],
            'value': county.get('population', county.get('value', 0))
        }
    
        if 'average_age' in county:
            county_data['average_age'] = county['average_age']
        #end if
        
        counties.append(county_data)
        total_population += county_data['value']
    #end fo
    
    return counties, total_population
#end get_counties

def get_cities_towns(data, province_name, county_name):
    
    """
    Backward compatibility function for existing code.
    
    Args:
        data (dict):         Raw JSON data
        province_name (str): Name of the province
        county_name (str):   Name of the county
        
    Returns:
        tuple: (cities_towns_list, total_population) or (None, None)
    """
    
    cities_towns     = []
    total_population = 0
    
    # Find the specified province
    target_province = None
    for province in data.get('provinces', []):
        if province['name'].lower() == province_name.lower():
            target_province = province
            break
        #end if
    #end for
    
    if not target_province:
        return None, None
    #end if
    
    # Find the specified county
    target_county = None
    for county in target_province.get('counties', []):
        if county['name'].lower() == county_name.lower():
            target_county = county
            
            break
        #end if
    #end for
    
    if not target_county:
        return None, None
    #end if
    
    # Extract cities/towns data
    if 'cities_towns' in target_county:
        for city_town in target_county['cities_towns']:
            city_town_data = {
                'name':  city_town['name'],
                'value': city_town.get('population', city_town.get('value', 0))
            }
            
            # Add optional fields if they exist
            optional_fields = [
                'average_age', 
                'male_population', 
                'female_population', 
                'male_children', 
                'female_children', 
                'marital_status_percentages'
            ]
            for field in optional_fields:
                if field in city_town:
                    city_town_data[field] = city_town[field]
            #end for
            
            cities_towns.append(city_town_data)
            total_population += city_town_data['value']
        #end for
    #end if
    return cities_towns, total_population
#end get_cities_towns