# Synthetic Demographic Data Generator.

So ye, this did not start with this, it was more a lets create a couple of people for the local Locale thats more like real people, families (well it was idea out of my Neo4J / GraphDB blog).

And it simply grew from there (aka another Rabbit Hole). 

To make sure I don't trip over anyony locally looking, having problems I modelled the currently example as per Ireland. But if you look at the data folder, containing `data/ie_banks.json` and `data/ireland.json` you will realise it wont take much to model to your own locale.

At the moment this version will post data directly into either a PostgreSQL or MongoDB datastore. That is intentional, based on personal requirements, but sure it can be extended to cover other db's or even Kafka cluster or other options.

Everything starts with brining up the base environment first and then the App. 

NOTE: those that follow my blogs, I've discovered a bug in my logger function which has been fixed here, the file_level and console_levels has also been aligned with standards.

BLOG: [Synthentic Demographic Data Generator]()

GIT REPO: [Synthentic_Demographic_Generator](https://github.com/georgelza/Synthentic_Demographic_Generator.git)


**The base environment:**

- `devlab/docker-compose.yml` which can be brought online by executing below, (this itself will use `.env` for some variables).

- the `make run` as defined in

- `devlab/Makefile`

For more detail how to stand up the entire environment see the below **The App:**.


## The App:

To run the Generator, execute (from root directory):

- `python3 -m venv ./venv`

- `source venv/bin/activate`

- `pip install --upgrade pip`

- `pip install -r requirements`

- `cd infrastrcture`

- `make pull`

- `cd ../devlab/connect`

- `make build`

- `cd ../` -> Project root directory
  
- `run.sh` will use `.pws` for passwords etc. 


## Data structures used:

Below are some of the data structures used along the way, Aadditionally also see the `app/option_lists.py` for various structures and weightings, which drives how the data is distributed/selected at a not so exactly random bases.


**Adults:**

```
    _id
    Name
    Surname
    Genders
    DOB
    National Identity Number (i.e; PPS, SSN ZA ID Number)
    Account
    Marital Status
    Status (Living/Deceased)
    Address
```


**Children:**

```
    _id
    Name
    Surname
    Genders
    DOB
    National Identity Number (i.e; PPS, SSN ZA ID Number)
    Father 
    Mother
    Address
    family_id
```


**Family's:**

```
    _id
    Husband
    Wife
    Kids
    Address
```


**Addresses:**

- This is used as a sub structure in the above

```
    Country
        Provinces
            Counties
                Towns/Cities
                    Streets
                    Postal Code
```


**Account:**

- This is used as a sub structure in the above
  
```
    Bank detail
    or 
    Credit Card detail
```


**Banks:**

- This is the main file defining our banks, for Ireland it is the 3 primarily National Banks and then 3 international banks with major presence, components of it is extracted and used in the structures above.
  
See: `data/ie_banks.json`

```
    Name
    BIGFI
    SWIFT
    IBAN Prefix
    ...
    Account Types
        Saving
        Transaction
        Cheque
        Notice
        ...
```


**ireland.json**

- This is used to get the various provices, which contain multiple counties which contain multiple cities/towns, each with a name and a population and additional information.
  
For a full structure See: `data/ireland.json` for entire structure.
```
    Name
    Population
    ...
    Provinces
        Name
        Population
        ...
        Counties
            Name
            Population
            ...
            Towns/Cities
                Name
                Population
                ...

```


### Misc:

For the curious. There is `DBPerformance.md` and the associated `database_performance_dashboard.html`
This only covered the MongoDB and Postgress datastores, did this prior to adding Redis which was a good 75% faster on my local machine.


By: **George Leonard**
- [georgelza@gmail.com](georgelza@gmail.com)
- [LinkedIn Profile](https://www.linkedin.com/in/george-leonard-945b502/)
- [Medium Profile](https://medium.com/@georgelza)
