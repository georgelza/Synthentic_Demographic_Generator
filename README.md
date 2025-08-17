# Synthetic Demographic Data Generator.

So ye, this did not start with this, it was more a lets create a couple of people for the local Locale thats more like real people, families (well it was idea out of my Neo4J / GraphDB blog).

And it simply grew from there (aka another Rabbit Hole). 

To make sure I don't trip over anyony locally looking, having problems I modelled the currently example as per Ireland. But if you look at the data folder, containing `data/ie_banks.json` and `data/ireland.json` you will realise it wont take much to model to your own locale.

At the moment this version will post data directly into either a PostgreSQL or MongoDB datastore. That is intentional, based on personal requirements, but sure it can be extended to cover other db's or even Kafka cluster or other options.

Everything starts with brining up the base environment first and then the App. 

NOTE: those that follow my blogs, I've discovered a bug in my logger function which has been fixed here, the file_level and console_levels has also been aligned with standards.

BLOG: [Synthetic Demographic Population Data Generator](https://medium.com/@georgelza/synthetic-demographic-population-data-generator-3e6e526d7b9a)

GIT REPO: [Synthentic_Demographic_Generator](https://github.com/georgelza/Synthentic_Demographic_Generator.git)


**The base environment:**

- `devlab/docker-compose.yml` which can be brought online by executing below, (this itself will use `.env` for some variables).

- the `make run` as defined in

- `devlab/Makefile`

For more detail how to stand up the entire environment see the below **Running the App:**.


## Running the App:

To run the Generator, execute the following from root directory:

For more information re it's structure see `app/README.md`.

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

Example:
```
{
    "_id": "fa0a58a1-3fb9-4dc4-950d-27c96caad683",
    "name": "Lisa-Marie",
    "surname": "Hearty",
    "uniqueId": "1915492K",
    "gender": "F",
    "dob": "55/11/24",
    "marital_status": "Widowed",
    "partner": "5687475E",
    "status": "Deceased",
    "account": [
        {
            "bank": "Allied Irish Banks, p.l.c.",
            "bicfi_code": "AIBKIE2D",
            "swift_code": "AIBKIE2D",
            "accountNumber": "IE29AIBK93115289539755",
            "accountType": "Savings/Deposit"
        },
        {
            "card_holder": "L Hearty",
            "card_number": "4223843492070632",
            "exp_date": "11/25",
            "card_network": "Visa",
            "issuing_bank": "Permanent TSB"
        }
    ],
    "address": {
        "street": "39 Fitzpatrick Street Street",
        "town": "Limerick City",
        "county": "Limerick",
        "state": "Munster",
        "post_code": "X348394",
        "country": "Ireland"
    },
    "family_id": "344de5a9-fa58-4947-b887-73996047a228"
}

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

Example:
```
{
    "_id": "1b09d0fe-e57d-46a9-8be1-5f077abe33a2",
    "name": "Lewis",
    "surname": "O'Gara",
    "gender": "Male",
    "dob": "83/12/18",
    "uniqueId": "9144598B",
    "father_idNumber": "1395127Q",
    "mother_idNumber": "6997438K",
    "address": {
        "street": "59 Jackson Street Street",
        "town": "Fingal",
        "county": "Dublin",
        "state": "Leinster",
        "post_code": "E28 9K1C",
        "country": "Ireland"
    },
    "family_id": "c86bee1d-0b57-4dc4-abef-9215ecc0cb4c"
}
```

**Family's:**

```
    _id
    Husband
    Wife
    Kids
    Address
```

Example:
```
{
    "_id": "4004db21-05a7-4524-a990-1aa0203a0c24",
    "husband": {
        "name": "Oran",
        "surname": "Hardiman",
        "uniqueId": "6455409D",
        "gender": "M",
        "dob": "54/09/15",
        "marital_status": "Married",
        "partner": "4486096P",
        "status": "Living",
        "account": [
            {
                "bank": "Bank of Ireland",
                "bicfi_code": "BOFIIE2D",
                "swift_code": "BOFIIE2D",
                "accountNumber": "IE79BOFI90583835990934",
                "accountType": "Current Accounts"
            },
            {
                "bank": "Barclays Bank Ireland plc",
                "bicfi_code": "BARCIE2D",
                "swift_code": "BARCIE2D",
                "accountNumber": "IE19BARC90100417221297",
                "accountType": "Current Accounts"
            },
            {
                "bank": "Citibank Europe plc",
                "bicfi_code": "CITIIE2X",
                "swift_code": "CITIIE2X",
                "accountNumber": "IE53CITI99003336666053",
                "accountType": "Current Accounts"
            },
            {
                "bank": "Bank of Ireland",
                "bicfi_code": "BOFIIE2D",
                "swift_code": "BOFIIE2D",
                "accountNumber": "IE79BOFI90583879228923",
                "accountType": "Current Accounts"
            },
            {
                "bank": "Allied Irish Banks, p.l.c.",
                "bicfi_code": "AIBKIE2D",
                "swift_code": "AIBKIE2D",
                "accountNumber": "IE29AIBK93115217364160",
                "accountType": "Current Accounts"
            },
            {
                "bank": "Barclays Bank Ireland plc",
                "bicfi_code": "BARCIE2D",
                "swift_code": "BARCIE2D",
                "accountNumber": "IE19BARC90100495214836",
                "accountType": "Business Accounts"
            },
            {
                "card_holder": "O Hardiman",
                "card_number": "4295106831188124",
                "exp_date": "11/25",
                "card_network": "Visa",
                "issuing_bank": "Allied Irish Banks, p.l.c."
            }
        ]
    },
    "wife": {
        "name": "Paula",
        "surname": "Hardiman",
        "uniqueId": "4486096P",
        "gender": "F",
        "dob": "57/12/22",
        "marital_status": "Married",
        "partner": "6455409D",
        "status": "Living",
        "account": [
            {
                "bank": "Bank of America Europe DAC",
                "bicfi_code": "BOFAIE3X",
                "swift_code": "BOFAIE3X",
                "accountNumber": "IE58BOFA99006105822922",
                "accountType": "Savings/Deposit"
            },
            {
                "bank": "Bank of Ireland",
                "bicfi_code": "BOFIIE2D",
                "swift_code": "BOFIIE2D",
                "accountNumber": "IE79BOFI90583844975923",
                "accountType": "Current Accounts"
            },
            {
                "card_holder": "P Hardiman",
                "card_number": "4197997192488510",
                "exp_date": "11/25",
                "card_network": "Visa",
                "issuing_bank": "Permanent TSB"
            },
            {
                "card_holder": "P Hardiman",
                "card_number": "4341141958700744",
                "exp_date": "08/27",
                "card_network": "Visa",
                "issuing_bank": "Permanent TSB"
            }
        ]
    },
    "address": {
        "street": "78 Moy Street Street",
        "town": "Limerick City",
        "county": "Limerick",
        "state": "Munster",
        "post_code": "C98FCAC",
        "country": "Ireland"
    }
}

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
Example:
```
{
    "street": "78 Moy Street Street",
    "town": "Limerick City",
    "county": "Limerick",
    "state": "Munster",
    "post_code": "C98FCAC",
    "country": "Ireland"
}

```

**Account:**

- This is used as a sub structure in the above
  
```
    Bank detail
    or 
    Credit Card detail
```

Example

```
[
    {
        "bank": "Bank of America Europe DAC",
        "bicfi_code": "BOFAIE3X",
        "swift_code": "BOFAIE3X",
        "accountNumber": "IE58BOFA99006105822922",
        "accountType": "Savings/Deposit"
    },
    {
        "bank": "Bank of Ireland",
        "bicfi_code": "BOFIIE2D",
        "swift_code": "BOFIIE2D",
        "accountNumber": "IE79BOFI90583844975923",
        "accountType": "Current Accounts"
    },
    {
        "card_holder": "P Hardiman",
        "card_number": "4197997192488510",
        "exp_date": "11/25",
        "card_network": "Visa",
        "issuing_bank": "Permanent TSB"
    },
    {
        "card_holder": "P Hardiman",
        "card_number": "4341141958700744",
        "exp_date": "08/27",
        "card_network": "Visa",
        "issuing_bank": "Permanent TSB"
    }
]

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
