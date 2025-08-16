#######################################################################################################################
#
#
#  	Project     	: 	Generic Data generator.
#
#   File            :   option_lists.py
#
#   Description     :   Create a dataset representing a demographic distribution
#
#   Created     	:   06 Aug 2025
#
#                   :   The below lists are used by weighted_random packages as the option lists.
#
#   As per 2022 Census
#
#   kids_options
#       ~42% - of families have no children
#       ~26% - have 1 child
#       ~22% - have 2 children  
#       ~7%  - have 3 children
#       ~2%  - have 4 children
#       ~1%  - have 5+ children
#
#   marital_options
#       43% - of population aged 15+ (never married)
#       46% - (including remarried & civil partnerships)  
#       ~3% - (part of the 6% separated/divorced)
#       ~3% - (part of the 6% separated/divorced)
#       ~5% - (remaining percentage to total 100%)
#
#   gender_options
#       49.4% - (2,544,549 males)
#       50.6% - (2,604,590 femal
#
#   children_yn_options
#       ~69% - of families have children
#       ~31% - of families have no children
#
#   accountTypes_options
#       55% - Higher - most common account type
#       30% - Moderate share
#       15% - Lower - smaller segment
#
#   bank_accounts_per_person 
#       35% - Single account holders
#       40% - Most common (main + savings)
#       18% - Main + savings + specialty
#       7%  - Multiple accounts for various purposes
#
#       1-3  - accounts (80%): The vast majority of Irish consumers
#       6-10 - accounts (5%):  Specialized users
#       
#   age_distribution_ireland - We cheat here a bit, we only model the adults, children wil be on top of this number,
#                               if you want to make it more accurate then simply subtract the number of children.
#       0.17 -  20-29 years - 875353
#       0.20 -  30-39 years - 1029827
#       0.17 -  40-49 years - 875353
#       0.19 -  50-59 years - 978336
#       0.16 -  60-69 years - 823862
#       0.11 -  70-79 years - 566405
#
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.1"
__copyright__   = "Copyright 2025, - George Leonard"


gender_options = [
    {"name": "Male",    "value": 0.494},
    {"name": "Female",  "value": 0.506}
]


marital_options = [
    {"name": "Single",      "value": 0.43},
    {"name": "Married",     "value": 0.46}, 
    {"name": "Separated",   "value": 0.03},
    {"name": "Divorced",    "value": 0.03},
    {"name": "Widowed",     "value": 0.05}
]


children_yn_options = [
    {"name": 0, "value": 0.31},
    {"name": 1, "value": 0.69}
]


kids_options = [
    {"name": 0, "value": 0.42}, 
    {"name": 1, "value": 0.26},
    {"name": 2, "value": 0.22}, 
    {"name": 3, "value": 0.07}, 
    {"name": 4, "value": 0.02},
    {"name": 5, "value": 0.01}
]


motherCustody_options = [
    {"name": 0, "value": 0.1},
    {"name": 1, "value": 0.9}
]


livingstatus_yn_options = [
    {"name": "Living",   "value": 0.85}, 
    {"name": "Deceased", "value": 0.15}
]


banks_options = [
    {"name": "Allied Irish Banks, p.l.c.",  "value": 0.200}, 
    {"name": "Bank of Ireland",             "value": 0.230},
    {"name": "Permanent TSB",               "value": 0.385},
    {"name": "Citibank Europe plc",         "value": 0.215},
    {"name": "Barclays Bank Ireland plc",   "value": 0.200},
    {"name": "Bank of America Europe DAC",  "value": 0.100}
]


cc_options = [
    {"name": "Mastercard",  "value": 0.4}, 
    {"name": "Visa",        "value": 0.6}
]


accountTypes_options = [
    {"name": "Current Accounts",    "value": 0.55},
    {"name": "Savings/Deposit",     "value": 0.30},
    {"name": "Business Accounts",   "value": 0.15}
]


bank_accounts_per_person = [
    {"name": 1, "value": 0.35},
    {"name": 2, "value": 0.29},
    {"name": 3, "value": 0.18},
    {"name": 4, "value": 0.08},
    {"name": 5, "value": 0.05},
    {"name": 6, "value": 0.03},
    {"name": 7, "value": 0.02}
]


credit_cards_per_person = [
    {"name": 1, "value": 0.65},  
    {"name": 2, "value": 0.25},  
    {"name": 3, "value": 0.10}  
]


age_distribution = [
    {"name": 20, "value": 0.17, "count": 875353},
    {"name": 30, "value": 0.20, "count": 1029827},
    {"name": 40, "value": 0.17, "count": 875353},
    {"name": 50, "value": 0.19, "count": 978336},
    {"name": 60, "value": 0.16, "count": 823862},
    {"name": 70, "value": 0.11, "count": 566405}
]

xage_distribution = [
    {"name": 20, "value": 0.17, "count": 875353},
    {"name": 30, "value": 0.20, "count": 1029827},
]