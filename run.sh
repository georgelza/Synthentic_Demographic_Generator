#!/bin/bash

. ./.pws

#export LOCALE=zu_ZA
export LOCALE=en_IE                             # used by faker to localise the name/surnames and addresses generated.
export COUNTRY=Ireland                          # Just used as the country tag in our address structure.
export ECHOCONFIG=1                             # Print to screen our current values as per this file, might make sense to have this as it goes into the log files.
export ECHORECORDS=0                            # If you want to see everything fly by, this will slow things down!!!
export RECCAP=5000000                           # If we're playing around and want to cap the records per age bracket, per day.      
export BLOCKSIZE=10                             # Age block size, 20-30 and 30-40 or 20-25 and 25-30
export BATCHSIZE=400
export RECCAP=9999999999999999999999            # Record Cap per Execution Block, if this number is > than BATCHSIZE then the batch will complete.
#export RECCAP=300
# Setting it to this impossible number will make it load the entire file, otherwise we will add logic to exit out at the number specified.


export CONSOLE_DEBUGLEVEL=20                    # Console Handler
export FILE_DEBUGLEVEL=20                       # File Handler
# logging.CRITICAL: 50
# logging.ERROR: 40
# logging.WARNING: 30
# logging.INFO: 20
# logging.DEBUG: 10
# logging.NOTSET: 0

export LOGDIR=logs                              # The log files is a concat of the database and the date/time.
export DATADIR=data                             # directy off root where these seed data files will be found
export DATASEEDFILE=ireland.json
export BANKSEEDFILE=ie_banks.json

export AGE_GAP=19                               # This drives the approx age difference between the mother and the children.
export VARIATION=2.5
export VARIATION_PERC=12                        # 12 = .12 = 12%

export DEST=3
# 0 no DB send
# 1 MongoDB
# 2 PostgreSQL
# 3 Redis
# 4 Kafka - ToBe

# PostgreSQL CDC Source
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
# export POSTGRES_USER=dbadmin
# export POSTGRES_PASSWORD=dbpassword
# export POSTGRES_CDC_USER=flinkcdc
# export POSTGRES_CDC_PASSWORD=dbpassword
export POSTGRES_DB=distro
export POSTGRES_ADULTS_TABLE=adults
export POSTGRES_CHILDREN_TABLE=children
export POSTGRES_FAMILY_TABLE=families

# MongoDB
export MONGO_ROOT=mongodb
export MONGO_USERNAME=
export MONGO_PASSWORD=
export MONGO_HOST=localhost
export MONGO_PORT=27017
export MONGO_DIRECT=directConnection=true 
export MONGO_DATASTORE=distro         
export MONGO_ADULTS_COLLECTION=adults
export MONGO_CHILDREN_COLLECTION=children
export MONGO_FAMILY_COLLECTION=families

# REDIS
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0
export REDIS_PASSWORD=
export REDIS_SSL=0
export REDIS_SSL_CERT=
export REDIS_SSL_KEY=
export REDIS_SSL_CA=

python3 app/main.py
