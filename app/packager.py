#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   packager.py
#
#   Description     :   Create a dataset representing a demographic distribution
#
#   Created     	:   06 Aug 2025
#
#   Functions       :   packageChild
#                   :   packageAdults
#
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.1"
__copyright__   = "Copyright 2025, - George Leonard"


import uuid
from utils import *
from option_lists import *
from getAccount import *
from getIDNumber import *
from weighted_random import *


def packageChild(fake, config_params, childPackage):
    
    childGender        = WeightedRandomSelector(gender_options, scale=1.0).get_random()
    childDOB           = generate_birth_date(childPackage["femaleDOB"], childPackage["ageGap"], childPackage["variation"])
    
    if childGender == "Male":
        childIdNumber  = generate_IdNumbers(fake, config_params, childDOB, "Male", 1)[0]
        firstName      = fake.first_name_male()
        
    else:
        childIdNumber  = generate_IdNumbers(fake, config_params, childDOB, "Female", 1)[0]
        firstName      = fake.first_name_female()

    #end if
    
    child_a = {             # Part of Family document - Still include Parent idNumbers, for coverage when marital_status = Divorce to reference the parent.
        "name":             firstName,
        "surname":          childPackage["surname"],
        "gender":           childGender,
        "dob":              childDOB,
        "uniqueId":         childIdNumber,
        "father_idNumber":  childPackage["maleId"],
        "mother_idNumber":  childPackage["femaleId"]    }

    child_b = {             # Stand alone children table/collection
        "_id":              str(uuid.uuid4()),
        "name":             firstName,
        "surname":          childPackage["surname"],
        "gender":           childGender,
        "dob":              childDOB,
        "uniqueId":         childIdNumber,
        "father_idNumber":  childPackage["maleId"],
        "mother_idNumber":  childPackage["femaleId"],
        "address":          childPackage["address"],
        "family_id":        childPackage["family_id"]
    }
    
    return child_a, child_b
# end packageChild


def packageAdults(fake, familyPackage, mylogger):
    
    male_firstName     = fake.first_name_male()
    female_firstName   = fake.first_name_female()

    m_accounts = createBankAccount(fake, male_firstName[0],   familyPackage["m_surname"])
    f_accounts = createBankAccount(fake, female_firstName[0], familyPackage["f_surname"])

    # Generate unique MongoDB _id values
    male_mongo_id   = str(uuid.uuid4())
    female_mongo_id = str(uuid.uuid4())

    # The _b records are inserted into the adults collection, whereas the _a are added to the family structure that has it's owned _id field.
    if familyPackage["male_livingstatus_status"] == "Living":
        
        male_a = {               # Family Package
            "name":              male_firstName,
            "surname":           familyPackage["m_surname"],
            "uniqueId":          familyPackage["maleId"],
            "gender":            "M",
            "dob":               familyPackage["maleDOB"],
            "marital_status":    familyPackage["marital_status"],
            "partner":           familyPackage["femaleId"],
            "status":            familyPackage["male_livingstatus_status"],   
            "account":           m_accounts                 
        }

        male_b = {               # Standalone Package
            "_id":               male_mongo_id,  # Use UUID for MongoDB _id
            "name":              male_firstName,
            "surname":           familyPackage["m_surname"],
            "uniqueId":          familyPackage["maleId"],
            "gender":            "M",
            "dob":               familyPackage["maleDOB"],
            "marital_status":    familyPackage["marital_status"],
            "partner":           familyPackage["femaleId"],
            "status":            familyPackage["male_livingstatus_status"],
            "account":           m_accounts,
            "address":           familyPackage["m_address"],
            "family_id":         familyPackage["family_id"]          
        }
    else:                        
        male_a = {               # Family Package - Deceased
            "name":              male_firstName,
            "surname":           familyPackage["m_surname"],
            "uniqueId":          familyPackage["maleId"],
            "gender":            "M",
            "dob":               familyPackage["maleDOB"],
            "marital_status":    familyPackage["marital_status"],
            "partner":           familyPackage["femaleId"],
            "status":            familyPackage["male_livingstatus_status"],      
            "account":           f_accounts                 
        }

        male_b = {
            "_id":               male_mongo_id,  # Use UUID for MongoDB _id
            "name":              male_firstName,
            "surname":           familyPackage["m_surname"],
            "uniqueId":          familyPackage["maleId"],
            "gender":            "M",
            "dob":               familyPackage["maleDOB"],
            "partner":           familyPackage["femaleId"],
            "status":            familyPackage["male_livingstatus_status"],
            "account":           f_accounts,
            "address":           familyPackage["m_address"],
            "family_id":         familyPackage["family_id"]          
        }

    if familyPackage["female_livingstatus_status"] == "Living":
        female_a = {
            "name":              female_firstName,
            "surname":           familyPackage["f_surname"],
            "uniqueId":          familyPackage["femaleId"],
            "gender":            "F",
            "dob":               familyPackage["femaleDOB"],
            "marital_status":    familyPackage["marital_status"],
            "partner":           familyPackage["maleId"],
            "status":            familyPackage["female_livingstatus_status"],     
            "account":           f_accounts                 
        }

        female_b = {
            "_id":               female_mongo_id,  # Use UUID for MongoDB _id
            "name":              female_firstName,
            "surname":           familyPackage["f_surname"],
            "uniqueId":          familyPackage["femaleId"],
            "gender":            "F",
            "dob":               familyPackage["femaleDOB"],
            "marital_status":    familyPackage["marital_status"],
            "partner":           familyPackage["maleId"],
            "status":            familyPackage["female_livingstatus_status"],
            "account":           f_accounts,          
            "address":           familyPackage["f_address"], 
            "family_id":         familyPackage["family_id"]          
        }
    else:
        female_a = {
            "name":              female_firstName,
            "surname":           familyPackage["f_surname"],
            "uniqueId":          familyPackage["femaleId"],
            "gender":            "F",
            "dob":               familyPackage["femaleDOB"],
            "marital_status":    familyPackage["marital_status"],
            "partner":           familyPackage["maleId"],
            "status":            familyPackage["female_livingstatus_status"],     
            "account":           f_accounts                 
        }

        female_b = {
            "_id":               female_mongo_id,  # Use UUID for MongoDB _id
            "name":              female_firstName,
            "surname":           familyPackage["f_surname"],
            "uniqueId":          familyPackage["femaleId"],
            "gender":            "F",
            "dob":               familyPackage["femaleDOB"],
            "marital_status":    familyPackage["marital_status"],
            "partner":           familyPackage["maleId"],
            "status":            familyPackage["female_livingstatus_status"],
            "account":           f_accounts,             
            "address":           familyPackage["f_address"], 
            "family_id":         familyPackage["family_id"]          
        }
               
    return male_a, female_a, male_b, female_b
#end packageAdults