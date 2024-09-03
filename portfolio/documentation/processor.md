## Detailed Breakdown of Functions

### 1. **`process_portfolio(validated_data)`:**

- **Purpose:** Orchestrates the entire portfolio processing pipeline.
- **Steps:**
  - Updates company base DataFrame with additional information.
  - Constructs a dictionary mapping companies to their signal generation DataFrames.
  - Calculates PNL for the entire portfolio using `formulate_PNL_df`.
  - Summarizes daily PNL for each company using `formulate_daily_pnl_summary`.
  - Updates the company base DataFrame with additional PNL metrics.
  - Saves the PNL and daily summary DataFrames to CSV files.

### 2. **`update_company_summary(validated_data, summary_df)`:**

- **Purpose:** Updates the company base DataFrame with PNL metrics based on the daily summary DataFrame.
- **Steps:**
  - Loops through each company in the company base DataFrame.
  - Retrieves entry and exit data for the company from the summary DataFrame.
  - Calculates and updates metrics like Open Exposure Cost, MTM Unrealized, MTM Realized, etc. in the company base DataFrame.
  - Calculates and updates daily realized gain and closing capital for the entire portfolio.

### 3. **`formulate_PNL_df(validated_data, company_sg_df_map)`:**

- **Purpose:** Creates a PNL DataFrame containing detailed information for each transaction.
- **Steps:**
  - Uses a `defaultdict` to accumulate data for each company and date.
  - Iterates through the company base DataFrame grouped by date.
  - Loops through each company's signal generation DataFrame to process entries and exits.
    - Updates common record fields like Datetime, Company, Unique ID, etc.
    - Processes entries:
      - Adds entry-specific data like Entry ID, Entry Type, Purchase Price, etc.
      - Calculates volume based on Purchase Value and Entry Price.
      - Updates cumulative values (CUM_VALUE, CUM_VOLUME) considering entry volume and price.
    - Processes exits:
      - Adds exit-specific data like Exit ID, Exit Type, Sell Price, etc.
      - Calculates profit/loss based on Sell Value and cumulative values.
      - Updates cumulative values considering exit volume and price.
  - Returns the final PNL DataFrame.

### 4. **`fetch_ticker(company_tickers, company)`:**

- **Purpose:** Retrieves the ticker symbol for a company from a separate ticker mapping DataFrame.
- **Steps:**
  - Searches for the company in the ticker mapping DataFrame.
  - Returns the corresponding ticker symbol if found.
  - Returns "NA" if not found.

### 5. **`formulate_daily_pnl_summary(validated_data, pnl_df)`:**

- **Purpose:** Creates a DataFrame summarizing daily PNL for each company.
- **Steps:**
  - Uses a `defaultdict` to accumulate summary data for each company and date.
  - Iterates through the company base DataFrame grouped by date.
  - Loops through each company and retrieves their corresponding PNL data from the PNL DataFrame.
  - Uses separate dictionaries for entry and exit data to populate the summary DataFrame.
  - Calculates and adds metrics like Open Volume, Weighted Average Price, MTM Unrealized, etc.
  - Returns the final daily PNL summary DataFrame.

### 6. **`update_common_record(pnl_dict, company_row, date_time, instrument)`:**

- **Purpose:** Adds common record fields like Datetime, Company, Unique ID, etc. to the PNL dictionary.
- **Steps:**
  - Appends the specified values to the PNL dictionary.

### 7. **`construct_company_signal_dictionary(validated_data)`:**

- **Purpose:** Creates a dictionary mapping companies to their processed signal generation DataFrames.
- **Steps:**
  - Reads signal generation files for each company based on a mapping.
  - Reshapes the DataFrame to long format for easier processing.
  - Separates the DataFrame into Entry and Exit types.
  - Drops duplicates within each type and concatenates them back.
  - Returns the dictionary with processed signal generation DataFrames.

### 8. **`process_out_of_list_exit(company, pnl_dict, cum_value)`:**

- **Purpose:** Handles situations where a company is no longer in the investment list but has open positions.
- **Steps:**
  - Adds empty entries for missing entry data in the PNL dictionary.
  - Simulates an exit at the day's closing price for the remaining volume.
  - Updates cumulative values (CUM_VALUE, CUM_VOLUME) and calculates profit/loss.

### 9. **`process_entry(name, row, pnl_dict, configs, company_row, entry_id)`:**

- **Purpose:** Processes entry events in the signal generation DataFrame.
- **Steps:**
  - Adds entry-specific data to the PNL dictionary.
  - Calculates volume based on Purchase Value and Entry Price.
  - Updates cumulative values (CUM_VALUE, CUM_VOLUME).
  - Checks for price exceeded and adjusts volume if necessary.
  - Initializes TP-related fields.

### 10. **`process_exit(name, row, pnl_dict, configs)`:**

- **Purpose:** Processes exit events in the signal generation DataFrame.
- **Steps:**
  - Adds exit-specific data to the PNL dictionary.
  - Updates cumulative values.
  - Handles TP exits by calculating TP-related fields.
  - Updates TP-related fields for current and previous entries.

### 11. **`update_company_base_df(company_df, configs)`:**

- **Purpose:** Updates the company base DataFrame with additional required columns.
- **Steps:**
  - Assigns unique IDs to each company.
  - Calculates risk per stock and category risk total.
  - Updates allowed exposure.
