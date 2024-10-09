# Trade Sheet Generation Module

## Overview

This module is designed for generating trade sheets across three market segments—**Cash**, **Futures**, and **Options**—based on input signals and market data. It processes trade entries and exits using segment-specific logic, leveraging input files like Signal Generated File, Expiry File, Strike File, and segment-specific Database Files.

## High-Level Flow

1. **Fetch Market Data**: Retrieve market data based on the signal's start and end times.
2. **Tracking Price Calculation**: Determine the tracking price using rules specific to each segment.
3. **Entry and Exit Price Calculation**: Calculate entry and exit prices using:
   - Appreciation/Depreciation conditions.
   - Target profit and stop-loss rules.
   - Expiry dates and Days-to-Exit (DTE).
4. **Trade Management**: Handle hedging, capital management, and volume calculations.

---

## Tracking Price Determination

The tracking price is the close price of the first candle (or the next available candle) within the signal's timeframe. The rules differ by market segment:

### 1. **Cash Segment**

- Use the **close price** at the signal start time from the Cash database.

### 2. **Futures Segment**

- Use the **close price** at the signal start time from the Futures database. If unavailable, use the first available close price within the signal timeframe.

### 3. **Options Segment**

- Fetch **cash_tracking_price** from the Cash database.
- Determine the strike price using the Strike File, based on expiry and conditions like At-The-Money (ATM), In-The-Money (ITM), or Out-The-Money (OTM).
- Use the Options database to find the corresponding Call (CE) or Put (PE) option and take the first available close price for that option as the tracking price.

---

## Entry Price Calculation

Entry prices depend on whether the Appreciation/Depreciation entry conditions are enabled or disabled:

1. **Appreciation/Depreciation Entry Off**:
   - The entry price is simply the tracking price.
2. **Appreciation/Depreciation Entry On**:
   - **Appreciation**: Enter when the high of the candle exceeds the appreciation price.
   - **Depreciation**: Enter when the low of the candle falls below the depreciation price.

---

## Exit Price Calculation

Exit prices can be determined by several factors:

1. **Target Profit Exit**: Triggered when user-defined profit targets are reached (if enabled).
2. **Stop Loss Exit**: Triggered when predefined stop-loss conditions are met.
3. **Signal Exit**: Exit when the signal’s end time is reached.
4. **Expiry Exit**: In Futures/Options, exit occurs on the expiry date (from the Expiry File).
5. **Days-to-Exit (DTE) Logic**: Apply Days-to-Exit rules to exit trades a specified number of days before expiry.
6. **Delayed Exit**: If no candle is available at the signal end time, the exit occurs at the first available candle after the signal end, but no later than the expiry.

---

## Segment-Specific Logic

### **Cash Segment**:

- The tracking price is the close of the first candle during the signal's timeframe from the Cash DB.

### **Futures Segment**:

- The tracking price is the close of the first candle from the Futures DB during the signal's timeframe.
- If the price is unavailable at the signal start time, the next available candle's close price within the timeframe is used.

### **Options Segment**:

- Fetch the **cash_tracking_price** from the Cash DB at the signal start time.
- Use the Strike File to calculate the strike price based on expiry.
- Derive the **At-The-Money (ATM)** price and assess whether a specific strike is **In-The-Money (ITM)** or **Out-The-Money (OTM)**.
- Fetch the relevant option (CE for Call, PE for Put) from the Options DB, and use the first available close price as the tracking price for that option.

### **Comparision**:

| **Feature**             | **Options**                                      | **Futures**                       | **Cash**                           |
| ----------------------- |--------------------------------------------------| --------------------------------- | ---------------------------------- |
| **Expiry Management**   | Yes (complex)                                    | Yes (simpler)                     | No                                 |
| **Rollover Logic**      | Yes                                              | Yes                               | No                                 |
| **Strike Price**        | Yes (adjusts based on premium and ITM/OTM logic) | No (strike prices don’t apply)    | No (not applicable)                |
| **Premium Adjustment**  | Yes (adjusts based on cash price differences)    | No                                | No                                 |
| **Hedging**             | No (not applicable)                              | Optional (simpler hedge logic)    | No                                 |
| **Filtering Data**      | Based on expiry and missing dates                | Based on expiry and missing dates | Based on entry and exit dates only |
| **Lot Size Management** | Yes (per expiry contract)                        | Yes (per expiry contract)         | No                                 |
| **Complexity**          | High                                             | Medium                            | Low                                |

### OPTION FLOW:

The `generate_trade_sheet` function you shared is responsible for generating a trade sheet for the **Options** segment based on signals. Here's an overview of the features and logic it handles:

### Key Features and Processes:

1. **Segment Data Initialization**:

   - Initializes an empty DataFrame (`self.segment_df`) where the relevant market data for the selected segment (Options) will be stored.

2. **Date-wise Processing**:

   - Loops through each row in `self.ee_df`, which presumably contains entry and exit signal data, processing one row at a time based on the dates (`entry_dt` and `exit_dt`).
   - The logic compares the current entry date (`entry_dt`) with the previous processed date and removes previous data to optimize performance.

3. **Expiry Data Management**:

   - Calls the `set_expiry_data` method to retrieve and assign expiry-related information (such as expiry dates, lot sizes, and strike price differences) for the current date.
   - Handles different expiry logic for the Options segment, including rolling over to the next expiry date when needed.

4. **Rollover Logic**:

   - Implements a rollover mechanism where a signal can be applied to multiple expiry contracts if the position remains open across expiry boundaries.
   - Assigns a unique rollover ID (`rid`) to track positions that roll over to subsequent expiry dates.

5. **Data Filtering for Missing Dates**:

   - Checks for missing market data within the entry and exit date range and reads any missing data from CSV files.
   - Only loads data for weekdays, skipping weekends (Saturday and Sunday).

6. **Cash Price Tracking**:

   - Retrieves the cash price (`cash_tracking_price`) and the time from a separate cash database (`cash_db_df`), used to calculate ATM based on cash price and 
   track the movement of the underlying asset during the trade.

7. **Strike Price Calculation**:

   - Calculates the at-the-money (ATM) strike price based on the cash price and the strike difference. This strike price can be adjusted depending on whether it's an in-the-money (ITM) or out-of-the-money (OTM) option.
   - If a specific strike is provided (`self.strike`), the function adjusts the strike price based on the ITM or OTM logic and further fine-tunes the strike based on premium features.

8. **Premium Adjustment (Optional)**:

   - If the premium feature is enabled (`self.premium`) and a valid strike is provided, the strike price is adjusted based on the difference between the current option price and the cash tracking price. This ensures that the strike is selected optimally based on the option's premium.

9. **Signal Iteration**:

   - The function applies the signal logic by iterating over the filtered data. It checks the entry and exit dates within the segment data to process the trade.
   - Outputs relevant trade data such as entry/exit times, prices, lot sizes, and any adjustments made due to rollover or expiry.

10. **Handling Delayed Exit**:

    - Implements logic to handle delayed exits and stops the trade if certain conditions (like DTE-based exit) are met. DTE (Days to Expiry) logic determines when a position should be exited based on how close the expiry is.

11. **Trade Data Collection**:

    - Collects the results of each trade iteration, including trade IDs, rollover IDs, and other relevant trade details. These results are stored in the `results` list.

12. **Rollover and Signal Expiry Management**:

    - Continues adjusting the trade by increasing the expiry number (`signal_expiry_number`) and updating the entry date (`ro_entry_dt`) until either the position is closed or the expiry rollover logic dictates otherwise.

13. **Saving Results**:
    - After processing all rows, the function converts the results into a DataFrame and saves it as a CSV file. The output file is secured with restricted permissions for privacy and security.

### Summary of Features:

- **Expiry Management**: Handles rolling over positions across different expiry dates for Options.
- **Lot Size and Strike Price**: Dynamically calculates lot size and strike price based on expiry data and market signals.
- **Cash Price and Premium Adjustment**: Tracks cash price movements and adjusts strike price based on premiums if applicable.
- **Data Filtering**: Reads missing data for the relevant trading dates and applies it to the segment DataFrame.
- **Premium Logic**: Implements optional logic to handle strike-based premium adjustments.
- **Rollover Logic**: Supports rolling over positions to the next expiry, incrementing expiry numbers, and adjusting entry/exit points accordingly.

### FUTURE:

The `generate_trade_sheet` function you've provided for the **Futures** segment is designed to handle trades and rollovers across futures contracts, similar to the Options function but with differences in logic suited for futures. Here's an overview of the key features and processes:

### Key Features and Processes:

1. **Segment and Hedge Data Initialization**:

   - Initializes two DataFrames: one for storing futures data (`self.segment_df`) and another for hedge data (`self.hedge_df`), which is used only if hedging is enabled (`self.is_hedge`).

2. **Date-wise Processing**:

   - Iterates over the rows in `self.ee_df`, which contains the entry and exit signals.
   - Processes trades by looking at entry (`entry_dt`) and exit (`exit_dt`) dates for each signal.

3. **Expiry Data Management**:

   - Uses `self.set_expiry_data` to assign the expiry-related data (expiry dates and lot sizes) for the current trading date (`current_date`).
   - The expiry data is based on expiry numbers, with logic to manage rollover to the next expiry contract when necessary.

4. **Rollover Logic**:

   - Implements a rollover mechanism that tracks a unique Rollover ID (`rid`) for positions that span multiple expiry contracts.
   - The expiry number is updated (`signal_expiry_number`) each time a rollover occurs, and the new expiry contract is used to continue the trade.
   - It calculates the entry date for the next expiry (`ro_entry_dt`) and continues rolling over until a valid exit signal is met.

5. **Data Filtering for Missing Dates**:

   - Filters out any missing dates between the entry and exit date ranges that fall on weekdays.
   - Reads missing data from CSV files if certain dates are not already tracked in `self.date_expiry_tracker`.

6. **Futures Expiry Ticker**:

   - Constructs the expiry ticker string (`expiry_in_ticker`) based on the symbol and the expiry number. For instance, a ticker may look like "NIFTY-II.NFO" where "II" is the Roman numeral representing the expiry number.
   - The `int_to_roman` function converts the expiry number into Roman numerals, which is common in futures tickers.

7. **Signal Processing**:

   - Once the relevant futures data is filtered, the `iterate_signal` function processes the trade based on the filtered data (`filtered_df`), considering factors such as lot size, expiry date, and rollover logic.
   - The output from this iteration includes important details like entry/exit prices, times, and other signal-based trade metrics.
   - Assigns a unique `TRADE_ID` and `ROLLOVER_ID` to each trade.

8. **Hedging (Optional)**:

   - If hedging is enabled (`self.is_hedge`), the function performs hedge calculations through the `hedge_calculation` method. It uses the current futures data and expiry information to adjust the position based on hedging strategies.
   - The hedge expiry number (`hedge_expiry_number`) is tracked and adjusted accordingly.

9. **Handling Exit Logic**:

   - Implements delayed exit logic, which determines the exit based on conditions like DTE (Days to Expiry) if the trade is supposed to exit near the expiry date.
   - The trade is rolled over only if the position is still open after the current contract’s exit conditions are met.

10. **Trade Data Collection**:
    - Collects the results of each trade iteration, including trade and rollover IDs, and appends them to a list (`results`).
    - This data is eventually converted into a DataFrame and saved as a CSV file for further analysis.

### Summary of Features:

- **Expiry Management**: Handles futures contracts expiry using expiry numbers and manages rollovers if the position spans multiple contracts.
- **Rollover Logic**: Tracks rollovers across expiry contracts, updating the entry date and expiry number for each rollover.
- **Lot Size and Expiry Ticker**: Dynamically calculates the lot size and expiry ticker based on the symbol and the current expiry number.
- **Data Filtering**: Filters the data for missing trading dates and retrieves them from CSV files if needed.
- **Hedging (Optional)**: Implements hedging strategies using hedge expiry numbers, adjusting positions based on hedge logic.
- **Signal Iteration**: Processes the trade signal by iterating over filtered futures data, calculating relevant trade metrics like entry/exit prices and times.
- **Exit Logic**: Handles different exit conditions, including delayed exit and DTE-based exits, to manage when to close positions.

### CASH:

The `generate_trade_sheet` function you've provided for the **Cash** segment is a simpler version compared to Options and Futures.

### Key Features and Processes:

1. **Filtering by Signal (Tag)**:

   - The function begins by filtering the `self.ee_df` DataFrame to only include rows where the `TAG` column equals `InputCols.GREEN`. This indicates that only certain signals (green signals) are considered for further processing.

2. **Reading CSV Data**:

   - `self.segment_df` is populated by calling the `self.read_csv_files_in_date_range()` method, which likely reads data over a specified date range. This method is essential for loading cash market data into the segment DataFrame.

3. **Handling Previous Dates**:

   - If there is a `previous_date` already processed (i.e., a date for a previous trade), the segment DataFrame is filtered to remove rows with dates earlier than the current entry date (`entry_dt`). This optimization ensures that the DataFrame only contains relevant data for the current and upcoming trades.

4. **Filtering Data by Entry and Exit Dates**:

   - The function uses the entry and exit dates (`entry_dt` and `exit_dt`) to filter the `self.segment_df` DataFrame, retrieving only the data within this date range. The result is stored in `filtered_df`, which contains the market data necessary for processing the trade signals between the given entry and exit.

5. **Processing Signals**:
   - For each filtered entry and exit, the `iterate_signal` function is called. This function processes the trade signals using the filtered data (`filtered_df`). The returned output contains the calculated results for the trade (such as prices, times, and trade execution details).
6. **Storing Results**:

   - The results of each trade are appended to a list (`results`). Each result is a combination of the original row (`row`) and the processed output from `iterate_signal`.
   - The `previous_date` variable is updated to the current `entry_dt.date()` to track the last processed date.

7. **Result DataFrame**:
   - Once all the trades are processed, a DataFrame (`result_df`) is created from the `results` list. The columns include both the original signal data (`self.ee_df` columns) and the processed result data (`self.result.keys()`).
8. **Removing Unnecessary Columns**:
   - The function drops the `TRADE_ID` and `ROLLOVER_ID` columns from the final DataFrame (`result_df`) as these are not relevant for cash segment trades (since there is no rollover logic involved in the cash segment).
9. **Saving the Results**:
   - The final `result_df` is saved as a CSV file (`self.output_file_name`), and appropriate file permissions are set.

### Summary of Features:

- **Signal Filtering**: Focuses on trades with a specific tag (e.g., green signals).
- **Date Range Data Management**: Loads market data for the required date ranges using CSV files.
- **Simple Date Filtering**: Removes previously processed data to optimize filtering for new trades.
- **Signal Iteration**: Processes trades between the entry and exit dates using the `iterate_signal` function.
- **Simplified Trade Logic**: No expiry, strike, or rollover logic is involved, as the cash segment doesn't have these complexities.
- **Result Generation**: Collects trade results and outputs them into a CSV file after dropping unnecessary columns (like trade and rollover IDs).
