## Irish Provincial/county/town Seed Data

Below is a summary of our seed records in our ireland.json seed file.

I've based this on the Irish Census dataset from the 2022 for all 26 Counties

- This is modelled out in ireland.json

### Leinster Province (12 counties):
Dublin    - 10 cities/towns (all >10k except smaller ones included)
Louth     - 2 cities/towns (both >10k)
Meath     - 2 cities/towns (Navan >10k, Trim <10k)
Wicklow   - 2 cities/towns (Wicklow >10k, Rathdrum <10k)
Kildare   - 5 cities/towns (4 >10k, 1 at 14k+)
Kilkenny  - 2 cities/towns (Kilkenny >10k, Callan <10k)
Carlow    - 2 cities/towns (Carlow >10k, Tullow <10k)
Laois     - 2 cities/towns (Portlaoise >10k, Mountmellick <10k)
Offaly    - 3 cities/towns (Tullamore >10k, Birr <10k, Dungarvan >10k)
Westmeath - 2 cities/towns (Athlone, Mullingar, Moate)
Longford  - 2 cities/towns (Longford >10k, Granard <10k)
Wexford   - 2 cities/towns (Wexford >10k, Gorey <10k)


### Munster Province (6 counties):
Cork      - 5 cities/towns (Cork City, Carrigaline, Cobh, Mallow)
Kerry     - 4 cities/towns (Tralee, Killarney, Listowel, Cahersiveen)
Limerick  - 3 cities/towns (Limerick City, Newcastle West, Rathkeale)
Clare     - 4 cities/towns (Ennis, Shannon, Kilrush, Ennistymon)
Tipperary - 3 cities/towns (Clonmel >10k, Thurles <10k, Nenagh)
Waterford - 5 cities/towns (Waterford City, Tramore, Dungarvan, Lismore, Cappoquin)


### Connacht Province (5 counties):
Galway    - 2 cities/towns (Galway City >10k, Tuam <10k)
Mayo      - 2 cities/towns (Castlebar >10k, Ballina >10k)
Roscommon - 2 cities/towns (Roscommon >10k, Boyle <10k)
Sligo     - 2 cities/towns (Sligo >10k, Enniscrone <10k)
Leitrim   - 2 cities/towns (Carrick-on-Shannon <10k, Manorhamilton <10k)


### Ulster Province (3 counties):
Donegal   - 2 cities/towns (Letterkenny >10k, Buncrana <10k)
Cavan     - 2 cities/towns (Cavan >10k, Bailieborough <10k)
Monaghan  - 2 cities/towns (Monaghan <10k, Castleblayney <10k)



## Irish Banks

- This is modelled out in ie_banks.json
  
Using the IBAN numbers for the banks together with the below public available detail we can create a Python Faker provider that generate unique IBAN based account numbers per bank, as approximate to real as we can get, which we can then assign to the adults being created.


### Market Share

Approximate market share for the major retail banks in Ireland, based on recent data, is as follows:

**National Banks**

- Bank of Ireland: ~21.74%

- Allied Irish Banks (AIB): ~19.04%

- Permanent TSB (PTSB): ~3.88%

**International Banks**

- Citibank Europe plc: CITIIE2X: 21.59%

- Barclays Bank Ireland plc: BARCIE2D: 19.92%

- Bank of America Europe DAC: BOFAIE3X: 10.60%


### Key Contextual Information

It's important to note that these figures represent market share by total assets and not necessarily by the number of customers. The retail banking landscape in Ireland has changed significantly in recent years due to the exit of two major foreign-owned banks, Ulster Bank and KBC Bank Ireland, from the market. This has led to a redistribution of customers and a consolidation of market share among the remaining players.

The "48 banks" operating in Ireland, as mentioned previously, includes many specialized credit institutions and branches of foreign banks (like Citibank and Barclays) that do not have a significant presence in the day-to-day retail banking market for individual consumers. The figures above are therefore most relevant to the domestic retail banking sector


### Account Types

The main retail banks in Ireland, such as AIB, Bank of Ireland, and Permanent TSB, generally offer the following categories of accounts:

- Current Accounts: These are for day-to-day banking, used for salaries, paying bills, and spending with a debit card. They often come in various versions, such as student, graduate, and basic accounts.

- Savings/Deposit Accounts: These are for saving money and earning interest. They come with different access rules, such as instant access, fixed-term, or regular saver accounts.

- Business Accounts: These are tailored for companies of all sizes, from sole traders to large corporations, with features like business overdrafts and dedicated support.


### IBAN Structure

Account Number Structure

While the full account number structure isn't public, banks in Ireland and across Europe follow a standardized format for international payments, known as the International Bank Account Number (IBAN). This is the only public-facing structure you'll find.

An Irish IBAN is a 22-character string that follows a specific format:

- Country Code (2 letters): IE for Ireland.

- Check Digits (2 numbers): Used to validate the IBAN for accuracy.

- Bank Identifier Code (4 letters): The SWIFT/BIC code for the bank. For example, AIBK for Allied Irish Banks or BOFI for Bank of Ireland.

- National Sort Code (6 numbers): Identifies the bank and branch.

- Account Number (8 numbers): The individual account number.

So, while the banks' internal account numbers are not standardized, their public-facing IBANs are. This ensures that payments can be routed correctly both domestically and international