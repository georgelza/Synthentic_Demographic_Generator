#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   getIDNumber.py
#
#   Description     :   Create a dataset representing a demographic distribution
#
#   Created     	:   06 Aug 2025
#
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.1"
__copyright__   = "Copyright 2025, - George Leonard"


def generate_IdNumbers(fake, config_params, dob, gender, cnt):

    idNumbers = []
    x         = 0

    if config_params["LOCALE"] == "en_IE":
         while x < cnt:
             idNumbers.append(fake.unique.pps_number())
             x += 1
            
        # #end while
    elif config_params["LOCALE"] == "zu_ZA":
         while x < cnt:
             idNumbers.append(fake.unique.sa_id_number(birth_date=dob, gender=gender))
             x += 1

        # #end while
    #end if

    return idNumbers
#enf generate_IdNumbers