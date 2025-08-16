#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   utils.py
#
#   Description     :   Generic utility routines
#
#   Created     	:   06 Aug 2025
#
#                       https://towardsdatascience.com/fake-almost-everything-with-faker-a88429c500f1/
#                       https://fakerjs.dev/guide/localization
#
#
#   Functions       :   getConfigs
#                   :   mylogger
#                   :   echo_config
#                   :   convert_yymmdd_to_date
#                   :   convert_date_to_yymmdd
#                   :   generate_birth_date
#                   :   pp_json
#                   :   load_jsondata
#
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.1"
__copyright__   = "Copyright 2025, - George Leonard"


import json
import random, os, logging
from datetime import datetime, timedelta


def getConfigs():
    
    config_params = {}
    # General
    config_params["ECHOCONFIG"]         = int(os.environ["ECHOCONFIG"])
    config_params["ECHORECORDS"]        = int(os.environ["ECHORECORDS"])
    config_params["BLOCKSIZE"]          = int(os.environ["BLOCKSIZE"])
    config_params["BATCHSIZE"]          = int(os.environ["BATCHSIZE"])
    config_params["RECCAP"]             = int(os.environ["RECCAP"])

    config_params["CONSOLE_DEBUGLEVEL"] = int(os.environ["CONSOLE_DEBUGLEVEL"])
    config_params["FILE_DEBUGLEVEL"]    = int(os.environ["FILE_DEBUGLEVEL"])

    config_params["COUNTRY"]                = os.environ["COUNTRY"]
    config_params["LOCALE"]                 = os.environ["LOCALE"]
    
    # Root of file name
    config_params["LOGDIR"]                 = os.environ["LOGDIR"]    
    config_params["DEST"]                   = int(os.environ["DEST"])
    
    if config_params["DEST"] == 1:
        config_params["LOGGINGFILE"]        = os.path.join(os.environ["LOGDIR"] , "mongo")
        
    elif config_params["DEST"] ==2:
        config_params["LOGGINGFILE"]        = os.path.join(os.environ["LOGDIR"] , "postgres")

    elif config_params["DEST"] ==3:
        config_params["LOGGINGFILE"]        = os.path.join(os.environ["LOGDIR"] , "redis")
    #end if

    # Seed data
    config_params["DATADIR"]                = os.environ["DATADIR"]     
    config_params["DATASEEDFILE"]           = os.path.join(os.environ["DATADIR"] , os.environ["DATASEEDFILE"])
    config_params["BANKSEEDFILE"]           = os.path.join(os.environ["DATADIR"] , os.environ["BANKSEEDFILE"])

    config_params["AGE_GAP"]                = int(os.environ["AGE_GAP"])
    config_params["VARIATION"]              = float(os.environ["VARIATION"])
    config_params["VARIATION_PERC"]         = int(os.environ["VARIATION_PERC"])

    # Mongo
    config_params["MONGO_HOST"]                 = os.environ["MONGO_HOST"]
    config_params["MONGO_PORT"]                 = os.environ["MONGO_PORT"]
    config_params["MONGO_DIRECT"]               = os.environ["MONGO_DIRECT"]
    config_params["MONGO_ROOT"]                 = os.environ["MONGO_ROOT"]
    config_params["MONGO_USERNAME"]             = os.environ["MONGO_USERNAME"]
    config_params["MONGO_PASSWORD"]             = os.environ["MONGO_PASSWORD"]
    config_params["MONGO_DATASTORE"]            = os.environ["MONGO_DATASTORE"]
    config_params["MONGO_ADULTS_COLLECTION"]    = os.environ["MONGO_ADULTS_COLLECTION"]
    config_params["MONGO_CHILDREN_COLLECTION"]  = os.environ["MONGO_CHILDREN_COLLECTION"]
    config_params["MONGO_FAMILY_COLLECTION"]    = os.environ["MONGO_FAMILY_COLLECTION"]

    # PostgreSQL
    config_params["POSTGRES_HOST"]              = os.environ["POSTGRES_HOST"]
    config_params["POSTGRES_PORT"]              = str(os.environ["POSTGRES_PORT"])
    config_params["POSTGRES_USER"]              = os.environ["POSTGRES_USER"]
    config_params["POSTGRES_PASSWORD"]          = os.environ["POSTGRES_PASSWORD"]
    config_params["POSTGRES_DB"]                = os.environ["POSTGRES_DB"]
    config_params["POSTGRES_ADULTS_TABLE"]      = os.environ["POSTGRES_ADULTS_TABLE"]
    config_params["POSTGRES_CHILDREN_TABLE"]    = os.environ["POSTGRES_CHILDREN_TABLE"]
    config_params["POSTGRES_FAMILY_TABLE"]      = os.environ["POSTGRES_FAMILY_TABLE"]

    # Redis
    config_params["REDIS_HOST"]                 = os.environ["REDIS_HOST"]
    config_params["REDIS_PORT"]                 = int(os.environ["REDIS_PORT"])
    config_params["REDIS_DB"]                   = int(os.environ["REDIS_DB"])
    config_params["REDIS_PASSWORD"]             = os.environ["REDIS_PASSWORD"]
    
    if int(os.environ["REDIS_SSL"]) == 0: config_params["REDIS_SSL"] = False 
    else: config_params["REDIS_SSL"] = True

    config_params["REDIS_SSL_CERT"]             = os.environ["REDIS_SSL_CERT"]
    config_params["REDIS_SSL_KEY"]              = os.environ["REDIS_SSL_KEY"]
    config_params["REDIS_SSL_CA"]               = os.environ["REDIS_SSL_CA"]

    
    return config_params
#end getConfig

def mylogger(filename, console_level, file_level):

    """
    Common Generic mylogger setup, used by master loop for console and file logging.
    """
    
    logger = logging.getLogger(__name__)
    
    # Set the overall logger level to the lowest of the two handlers
    lowest_level = min(console_level, file_level)
    logger.setLevel(lowest_level)
    
    # Create console handler
    ch = logging.StreamHandler()

    # create console handler
    ch = logging.StreamHandler()
    
    # Set console log level 
    if console_level == 10:
        ch.setLevel(logging.DEBUG)
        
    elif console_level == 20:
        ch.setLevel(logging.INFO)
        
    elif console_level == 30:
        ch.setLevel(logging.WARNING)
        
    elif console_level == 40:
        ch.setLevel(logging.ERROR)
      
    elif console_level == 50:
        ch.setLevel(logging.CRITICAL)
        
    else:   # == 0 aka logging.NOTSET
        ch.setLevel(logging.INFO)  # Default log level if undefined
        
    # Create a formatter
    ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(processName)s - %(message)s')

    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)


    # create file handler
    fh = logging.FileHandler(filename)

   # Set file log level 
    if console_level == 10:
        fh.setLevel(logging.DEBUG)
        
    elif console_level == 20:
        fh.setLevel(logging.INFO)
        
    elif console_level == 30:
        fh.setLevel(logging.WARNING)
        
    elif console_level == 40:
        fh.setLevel(logging.ERROR)
      
    elif console_level == 50:
        fh.setLevel(logging.CRITICAL)
        
    else:   # == 0 aka logging.NOTSET
        fh.setLevel(logging.INFO)  # Default log level if undefined
        
    # Create a formatter
    fh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)
    
    return logger
# end mylogger


def echo_config(config_params, mylogger):
    
    if config_params["ECHOCONFIG"] == 1:
        
        mylogger.info("***********************************************************")
        mylogger.info("* ")
        mylogger.info("*          Python ....")
        mylogger.info("* ")
        mylogger.info("***********************************************************")
        mylogger.info("* General")
        mylogger.info("* ")
        mylogger.info("* Console Debuglevel               : " + str(config_params["CONSOLE_DEBUGLEVEL"])) 
        mylogger.info("* File Debuglevel                  : " + str(config_params["FILE_DEBUGLEVEL"]))
        mylogger.info("* ")
    
        mylogger.info("* Locale                           : " + config_params["LOCALE"])
        mylogger.info("* Country                          : " + config_params["COUNTRY"])
        mylogger.info("* Seed Data Directory              : " + config_params["DATADIR"])
        mylogger.info("* Seed Data File                   : " + config_params["DATASEEDFILE"])
        mylogger.info("* Bank Data File                   : " + config_params["BANKSEEDFILE"])
        
        mylogger.info("* RecCap                           : " + str(config_params["RECCAP"]))
        mylogger.info("* Age Block Size                   : " + str(config_params["BLOCKSIZE"]))
        mylogger.info("* Batch Size                       : " + str(config_params["BATCHSIZE"]))
    
        mylogger.info("* ")        
        mylogger.info("* Log Directory                    : " + config_params["LOGDIR"])
        mylogger.info("* Log File                         : " + config_params["LOGGINGFILE"])

        mylogger.info("* ")
        mylogger.info("* DB Dest Specified                : " + str(config_params["DEST"]))
        if config_params["DEST"]   == 1: 
            mylogger.info("* DB Dest Specified                : MongoDB" )
            mylogger.info("* ")
            mylogger.info("* Mongo Root                       : " + config_params["MONGO_ROOT"])
            mylogger.info("* Mongo host                       : " + config_params["MONGO_HOST"])
            mylogger.info("* Mongo Port                       : " + str(config_params["MONGO_PORT"]))
            mylogger.info("* Mongo Direct                     : " + config_params["MONGO_DIRECT"])
            mylogger.info("* Mongo Datastore                  : " + config_params["MONGO_DATASTORE"])
            mylogger.info("* Mongo Collection                 : " + config_params["MONGO_ADULTS_COLLECTION"])
            mylogger.info("* Mongo Collection                 : " + config_params["MONGO_CHILDREN_COLLECTION"])
            mylogger.info("* Mongo Collection                 : " + config_params["MONGO_FAMILY_COLLECTION"])
            mylogger.info("* ")
            
        elif config_params["DEST"] == 2: 
            mylogger.info("* DB Dest Specified                : PostgreSQL" )
            mylogger.info("* PostgreSQL Host                  : " + config_params["POSTGRES_HOST"])
            mylogger.info("* PostgreSQL Port                  : " + str(config_params["POSTGRES_PORT"]))
            mylogger.info("* PostgreSQL DB                    : " + config_params["POSTGRES_DB"])
            mylogger.info("* PostgreSQL User                  : " + config_params["POSTGRES_USER"])
            mylogger.info("* PostgreSQL Password              : ************" )
            mylogger.info("* PostgreSQL Adultd                : " + config_params["POSTGRES_ADULTS_TABLE"])
            mylogger.info("* PostgreSQL Children              : " + config_params["POSTGRES_CHILDREN_TABLE"])
            mylogger.info("* PostgreSQL Families              : " + config_params["POSTGRES_FAMILY_TABLE"])

        elif config_params["DEST"] == 3: 
            mylogger.info("* DB Dest Specified                : Redis" )
            mylogger.info("* Redis Host                       : " + config_params["REDIS_HOST"])
            mylogger.info("* Redis Port                       : " + str(config_params["REDIS_PORT"]))
            mylogger.info("* Redis DB                         : " + str(config_params["REDIS_DB"])) 
            mylogger.info("* Redis Password                   : ************" )
            mylogger.info("* Redis SSL                        : " + str(config_params["REDIS_SSL"]))
            mylogger.info("* Redis SSL Cert                   : " + config_params["REDIS_SSL_CERT"])
            mylogger.info("* Redis SSL Key                    : " + config_params["REDIS_SSL_KEY"])
            mylogger.info("* Redis SSL CA                     : " + config_params["REDIS_SSL_CA"])

        mylogger.info("* ")
        mylogger.info("***********************************************************")     
        mylogger.info("")
# end echo_config


def convert_yymmdd_to_date(date_string):
    
    """
    Convert a YYMMDD string to formatted date YY/MM/DD
    
    Args:
        date_string (str): Date in YYMMDD format (e.g., "800610")
    
    Returns:
        str: Formatted date as YY/MM/DD (e.g., "80/06/10")
    
    Raises:
        ValueError: If input string is not exactly 6 digits
    """
    
    # Validate input
    if not date_string.isdigit() or len(date_string) != 6:
        raise ValueError("Input must be exactly 6 digits in YYMMDD format")
    
    #end if
    
    # Extract year, month, day
    yy = date_string[:2]
    mm = date_string[2:4]
    dd = date_string[4:6]
    
    # Basic validation for month and day ranges
    if not (1 <= int(mm) <= 12):
        raise ValueError(f"Invalid month: {mm}")
    
    #end if
    
    if not (1 <= int(dd) <= 31):
        raise ValueError(f"Invalid day: {dd}")
    
    #end if
    
    # Format as YY/MM/DD
    return f"{yy}/{mm}/{dd}"
# end convert_yymmdd_to_date


def convert_date_to_yymmdd(formatted_date):
    
    """
    Convert a formatted date YY/MM/DD to YYMMDD string
    
    Args:
        formatted_date (str): Date in YY/MM/DD format (e.g., "80/06/10")
    
    Returns:
        str: Date as YYMMDD string (e.g., "800610")
    
    Raises:
        ValueError: If input is not in correct YY/MM/DD format
    """
    
    # Validate input format
    if not isinstance(formatted_date, str) or formatted_date.count('/') != 2:
        raise ValueError("Input must be in YY/MM/DD format with exactly 2 forward slashes")
    
    # Split the date
    parts = formatted_date.split('/')
    
    if len(parts) != 3:
        raise ValueError("Input must be in YY/MM/DD format")
    
    #end if
    yy, mm, dd = parts
    
    # Validate each part is 2 digits
    if not (yy.isdigit() and len(yy) == 2):
        raise ValueError(f"Year must be exactly 2 digits: {yy}")
    
    #end if
    
    if not (mm.isdigit() and len(mm) == 2):
        raise ValueError(f"Month must be exactly 2 digits: {mm}")
    
    #end if
    
    if not (dd.isdigit() and len(dd) == 2):
        raise ValueError(f"Day must be exactly 2 digits: {dd}")
    
    #end if
    
    # Basic validation for month and day ranges
    if not (1 <= int(mm) <= 12):
        raise ValueError(f"Invalid month: {mm}")
    
    #end if
    
    if not (1 <= int(dd) <= 31):
        raise ValueError(f"Invalid day: {dd}")
    
    #end if
    
    # Combine into YYMMDD string
    return yy + mm + dd
#end convert_date_to_yymmdd


def generate_birth_date(parent_date_string, age_difference_years, std_deviation_years=2.5):
    
    """
    Generates a random birth date based on date and age difference.
    
    Args:
        i/e: parent_date_string (str): Parent's birth date in 'YY/MM/DD' format (e.g., '74/04/10')
        age_difference_years (int): Base age difference in years (e.g., 18)
        std_deviation_years (float): Standard deviation for random variation (default: 2.5)
        
        Can also be used to generate a wife's birth date approx around the husband.
    
    Returns:
        str: dependents's birth date in 'YY/MM/DD' format
    
    Example:
        >>> generate_child_birth_date('74/04/10', 18, 2.5)
        '91/08/23'  # Random date around 1992 +/- 2.5 years
    """
    
    # Parse the parent's birth date
    parent_date = datetime.strptime(parent_date_string, '%y/%m/%d')
    
    # Calculate the base child birth year
    base_child_year = parent_date.year + age_difference_years
    
    # Generate random variation using normal distribution
    # This gives us a range of approximately +/- 2.5 years around the base year
    year_variation = random.gauss(0, std_deviation_years)
    actual_child_year = int(base_child_year + year_variation)
    
    # Generate random month and day
    random_month = random.randint(1, 12)
    
    # Handle different month lengths and leap years
    if random_month in [1, 3, 5, 7, 8, 10, 12]:
        max_day = 31
        
    elif random_month in [4, 6, 9, 11]:
        max_day = 30
        
    else:  # February
        # Check for leap year
        if (actual_child_year % 4 == 0 and actual_child_year % 100 != 0) or (actual_child_year % 400 == 0):
            max_day = 29
            
        else:
            max_day = 28
    
    random_day = random.randint(1, max_day)
    
    # Create the child's birth date
    child_birth_date = datetime(actual_child_year, random_month, random_day)
    
    # Format and return as string
    return child_birth_date.strftime('%y/%m/%d')
#end generate_birth_date


def pp_json(json_thing, sort=True, indents=4):
    
    """
        Pretty Printer to JSON
    """
    
    if type(json_thing) is str:
        print(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
        
    else:
        print(json.dumps(json_thing, sort_keys=sort, indent=indents))

    #end if
    return None
# end pp_json
    

def load_jsondata(file_path, mylogger):
    
    """
    Loads JSON data from a file.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The loaded JSON data.
    
    More Reading:
        https://www.freecodecamp.org/news/loading-a-json-file-in-python-how-to-read-and-parse-json/
        https://www.geeksforgeeks.org/python/read-json-file-using-python/
        https://www.w3schools.com/python/python_json.asp
        https://realpython.com/python-json/
        https://flexiple.com/python/with-open-python-exception
    """
    
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            
        return data
        
    except FileNotFoundError as err:
        mylogger.err("File Not Found error: {file} {err}".format(
            file = file_path,
            err  = err
        ))

    except IOError as err:
        mylogger.err("An error occurred while reading the file: {file} {err}".format(
            file = file_path,
            err  = err
        ))
        
    except json.JSONDecodeError as err:
        mylogger.err("Could not decode JSON from: {file} {err}".format(
            file = file_path,
            err  = err
        ))
    
    except Exception as err:        
        mylogger.err("Generic File Read error: {file} {err}".format(
            file = file_path,
            err  = err
        ))
        
        return None
#end load_jsondata
