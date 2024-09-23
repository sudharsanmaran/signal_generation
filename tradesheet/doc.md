## Input Files:
Following will be input files to read to generate trade sheet.
- Signal generated file
- Expiry file
- Strike file
- Lot size file
- Database file: Database file is different for each segment.

## Segment:
Trade sheet generation is based on 3 segments:
- Cash
- Future
- Options

## Basic process:
- First we take one signal and its start date and end date.
- we have to fetch records from start date to end date from respected segment db. 
  - so for e.g. signal starts from 15/11/2023 9:15 and ends at 15/11/2023 9:34, then candles between these timestamp will be 
    fetched from segment database file. Segment db has the following columns:
    - ***Date, Time, Open, High, Low, Close, Volume and others***
### Steps (find_entry_exit Function):
ALL FORMULAS ARE IN FUNCTION.
- **Decide Tracking price**: this step will be different for each segment. basically the close of first candle will be tracking price.

- **Decide Entry Price**:
  - If **_Appreciation/Depreciation based entry_** is off, then consider tracking price as entry price.
  - If **_Appreciation/Depreciation based entry_** is on in user input, then based on tracking price we calculate,
    **_Appreciation/Depreciation price_** i.e. **_ad_price_**. Now, from next candle of tracking candle find entry based on following condition.
    - if appreciation is given then check for high > ad_price. For depreciation, check for low < ad_price. 
    - If this condition happens, take close of that candle as entry price.
- **Decide Exit Price**:
  - There are different type of exits (Exit type defined in **constants.ExitTypes**). Any exit condition we found first,
  we will take exit on that candle:
  - Target Profit Exit: consider only if user input for this is true, This is will not be considered at the time of redeployment. (Formula can be found in code) 
  - Stop Loss Exit: consider only if user input for this is true. (Formula can be found in code)
  - Signal Exit: If signal end time is equal to candle time, then take exit
  - Expiry Exit: In case of FUture/Option, there will be one input of expiry. we need to find expiry date from that number for current date in Expiry file.
    for ex: if current signal date is 1/11/2023 and expiry is 1 then in expiry file find 1/11/2023 and fetch date in column EXPIRY_01. 
  - DTE Based exit: For this there will be 2 input given, DTE number and time when we should exit. 
    DTE(Days to Exit) = expiry date - current date if expiry for 1/11/2023 is 3/11/2023 then DTE is 2.
    so, if user has input 2 for DTE based exit and time 9:24. then take exit on 9:24 when dte 2 in the signal.
  - Delayed Exit: When we do not find candle on signal end time, in segment db in that case we take exit on first candle from 
    after signal end candle to last candle of expiry date. (Find doc string of get_delayed_exit for more info)

## Find Tracking price:
To find tracking price of particular signal, always look in close column of DB file.
Let's say signal starts from 1/11/2022(1st november) 9:15 to 9:34, then
- ### Cash: 
  - tracking price will be close of 1/11/2022 9:15 in CASH DB

- ### Future: 
  - tracking price will be close of 1/11/2022 9:15 in FUTURE DB. If price is not found at 9:15 then first candle 
    after 9:15 up to 9:34 will be tracking price which is called revised tracking price.

- ### Options: 
  1. take a close price of 1/11/2022 9:15 from CASH DB let's call it cash_tracking_price.
  2. fetch strike diff from Strike file based on given input(expiry). so If expiry is given 1. then EXPIRY_01 column for
     1/11/2022 in strike file.
  3. Based on strike diff and cash_tracking_price, calculate ATM( refer get_atm_strike function).
  4. If strike is given in input, then calculate OTM or ITM based on ATM and strike diff(refer get_itm_or_otm function)
  5. let's call final price after 3rd and 4th step as strike_price
  6. Once we get strike price, we filter from Option db file of signal start date for the 
     combination of following string in ticker column: <expiry_date><strike_price><CE/PE>. 
      here, CE: CALL/GREEN, PE: PUT/RED.
  7. after filtering, tracking price will be close of 1/11/2022 9:15. If price is not found at 9:15 then first candle 
    after 9:15 up to 9:34 will be tracking price which is called revised tracking price.
  
  #### Premium feature:
  8. If this is **ON** in input and strike is > 0 i.e. positive, then we need to check further for tracking price.
  9. for example, strike is given 2. so we fetch strike diff for 2nd columns.
  10. For premium feature,
    - find difference between cash_tracking_price and strike_price.
    - if tracking price(on step 7) is less than difference of above step then select higher price. So, in this case
      strike will be 3. so find strike diff from strike file accordingly(i.e. column 3)
    - based on strike_price and new strike_diff, calculate new strike_price(logic is same as get_itm_or_otm function)
    - Repeat step from step 6.


### Next expiry trading
Input is 	
- **From which DTE**	5
- **Which Expiry**	2

And Expiry input is 1
so, for DTE less than equal to 5, we consider 2nd expiry else consider 1st expiry

### Volume:
  refer Docstring of sum_of_volume

### Capital Management
  refer Docstring of cal_capital_management
  
### AD Level Price
  Ad level price is price at ad_time in cash df. For example, signal starts from 1/11/2022(1st november) 9:15 to 9:34,
  and ad hits i.e. condition match for ad on 9:20 then the price in cash df at 9:20 will be ad_level_price. this will be 
  calculated for FUTURE and OPTIONS only.
  
### Hedge
- Hedge is calculated for FUTURE only. There are 2 inputs for hedge i.e. strike and expiry
- Once we find entry and exit time of trade for a signal in the future, we need to find hedge for that trade.
- so based on given hedge strike and expiry, we follow steps of finding trackig price from 1 to 7. (premium is not considered in hedge)
- once we filter db, we will find price at trade entry/exit time on that db.


##### Issues Found in qc:
1) Signal Exit time : 2/23/2023  1:15:00 PM  
   expiry Date: 2/23/2023
   Output Exit time is showing none. Issue was in delayed exit function. condition updated from  date_idx < len(date_ranges) to date_idx <= len(date_ranges)

2) Signal Exit time: 2/3/2023  9:15:00 AM
   expiry Date: 2/23/2023
   Output Exit time: 2/3/2023  9:15:00 AM
   Output Exit type: Delayed Exit.
  Exit type should be Signal exit as we have candle at 9:15. Issue was filtere_df was not taking 2/3/2023 date. And that was due to pd.date range function. In pd.Daterange function when you pass date with time it excludes end date.

3) 2nd issue same for options