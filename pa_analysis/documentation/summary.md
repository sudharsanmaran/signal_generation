## Function: process_summaries

1. **Purpose:** Reads and processes a list of file names, then returns the processed results.
2. **Parameters:**
   - `files` (list[str]): A list of file names to process.
3. **Returns:**
   - A dictionary with processed results for each file.

---

## Function: read_files

1. **Purpose:** Reads CSV files into DataFrames and processes them.
2. **Parameters:**
   - `files` (list[str]): A list of CSV file names.
3. **Returns:**
   - A dictionary where keys are file names and values are DataFrames.

---

## Function: get_masks

1. **Purpose:** Creates masks for filtering the DataFrame based on market direction and group positivity/negativity.
2. **Parameters:**
   - `df` (pd.DataFrame): The DataFrame to create masks for.
3. **Returns:**
   - Masks for filtering as a dictionary.

---

## Function: create_common_props

1. **Purpose:** Creates a dictionary of common properties for each summary.
2. **Parameters:**
   - `df` (pd.DataFrame): The DataFrame containing the summary data.
   - `terms` (list[str]): List of terms for the properties.
3. **Returns:**
   - A dictionary of common properties including instrument name, start and end dates, and duration.

---

## Function: create_category_result

1. **Purpose:** Creates a result dictionary for each category (overall, long, short).
2. **Parameters:**
   - `common_props` (dict): Dictionary of common properties.
   - `category` (str): The category for which to create results.
   - `df` (pd.DataFrame): The DataFrame with data for the category.
   - `mask` (pd.Series): Mask for filtering data.
   - `basic_result` (pd.DataFrame): Basic result DataFrame.
   - `basic_result_index_map` (dict): Index map for the basic results.
3. **Returns:**
   - A result dictionary with metrics like average and median price movements.

---

## Function: update_basic_analysis_summary

1. **Purpose:** Updates the basic analysis summary.
2. **Parameters:**
   - `df` (pd.DataFrame): The DataFrame with data.
   - `common_props` (dict): Dictionary of common properties.
   - `file` (str): File name associated with the summary.
   - `pos_neg_masks` (dict): Masks for positivity/negativity.
   - `direction_masks` (dict): Masks for market direction.
3. **Returns:**
   - Updated basic analysis summary.

---

## Function: update_first_cycle_summary

1. **Purpose:** Updates the first cycle summary with various statistics.
2. **Parameters:**
   - `df` (pd.DataFrame): The DataFrame with data.
   - `common_props` (dict): Dictionary of common properties.
   - `direction_masks` (dict): Masks for market direction.
   - `file` (str): File name associated with the summary.
   - `pos_neg_masks` (dict): Masks for positivity/negativity.
3. **Returns:**
   - Updated first cycle summary.

---

## Function: update_MTM_cycle_summary

1. **Purpose:** Updates the MTM (Mark-to-Market) cycle summary.
2. **Parameters:**
   - `df` (pd.DataFrame): The DataFrame with data.
   - `common_props` (dict): Dictionary of common properties.
   - `direction_masks` (dict): Masks for market direction.
   - `pos_neg_masks` (dict): Masks for positivity/negativity.
   - `file` (str): File name associated with the summary.
3. **Returns:**
   - Updated MTM cycle summary.

---

## Function: get_MTM_cycle_summary_multi_index

1. **Purpose:** Generates a MultiIndex for the MTM cycle summary DataFrame.
2. **Parameters:**
   - `include_fractal_cols` (bool): Whether to include fractal columns in the MultiIndex.
3. **Returns:**
   - A MultiIndex for the MTM cycle summary DataFrame.

---

## Function: get_first_cycle_summary_multi_index

1. **Purpose:** Generates a MultiIndex for the first cycle summary DataFrame.
2. **Parameters:**
   - `mtm_cols` (list[str]): List of MTM columns to include in the MultiIndex.
3. **Returns:**
   - A MultiIndex for the first cycle summary DataFrame.

---

## Function: update_MTM_crossed_count

1. **Purpose:** Updates the MTM crossed count for specified columns.
2. **Parameters:**
   - `df` (pd.DataFrame): The DataFrame with data.
   - `res` (dict): Results dictionary.
   - `mask` (pd.Series): Mask for filtering data.
   - `mtm_cols` (list[str]): List of MTM columns to update.
3. **Returns:**
   - Updated MTM crossed count.

---

## Function: get_basic_multi_index

1. **Purpose:** Generates a MultiIndex for the basic analysis summary DataFrame.
2. **Parameters:**
   - None
3. **Returns:**
   - A MultiIndex for the basic analysis summary DataFrame.

---
