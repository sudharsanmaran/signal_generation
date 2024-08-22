## Function: process_summaries

1. **Purpose:** Reads CSV files, processes their content, and writes the processed results to a summary CSV file.
2. **Parameters:**
   - `files` (List[str]): A list of file names to process.
3. **Returns:**
   - None

---

## Function: read_files

1. **Purpose:** Reads CSV files into DataFrames, parses date columns, and processes time-related columns.
2. **Parameters:**
   - `files` (List[str]): A list of CSV file names.
3. **Returns:**
   - List[pd.DataFrame]: A list of DataFrames corresponding to each CSV file.

---

## Function: get_multi_index

1. **Purpose:** Creates a MultiIndex for the DataFrame columns used in the summary.
2. **Parameters:**
   - None
3. **Returns:**
   - pd.MultiIndex: A MultiIndex object for DataFrame columns.

---

## Function: create_common_props

1. **Purpose:** Generates a dictionary of common properties for each summary entry, including strategy ID, instrument, duration, and other metrics.
2. **Parameters:**
   - `df` (pd.DataFrame): The DataFrame containing the data to summarize.
   - `terms` (List[str]): A list of terms derived from the file name used for generating properties.
3. **Returns:**
   - dict: A dictionary with common properties for the summary.

---

## Function: process_summary

1. **Purpose:** Processes a DataFrame and calculates various metrics based on the specified operations, then returns the processed results.
2. **Parameters:**
   - `df` (pd.DataFrame): The DataFrame to process.
   - `file` (str): The name of the file from which the DataFrame was read, used to derive some properties.
3. **Returns:**
   - List[dict]: A list of dictionaries with calculated metrics for each category.

---

## Function: handle_operation

1. **Purpose:** Handles the application of a specific operation (e.g., average, median) to a subset of the DataFrame and updates the result dictionary.
2. **Parameters:**
   - `operation` (Operations): The operation to apply (e.g., AVERAGE, MEDIAN).
   - `func` (callable): The function to use for the operation.
   - `sign` (str): Indicates whether to apply the operation to positive or negative data.
   - `mask` (pd.Series): Boolean mask for filtering the DataFrame.
   - `df` (pd.DataFrame): The DataFrame to process.
   - `key` (str): The column to operate on.
   - `col` (str): The name of the column in the result dictionary.
   - `res` (dict): The result dictionary to update.
3. **Returns:**
   - None

---

## Function: get_mask

1. **Purpose:** Creates masks for filtering the DataFrame based on category and market direction (positive or negative).
2. **Parameters:**
   - `df` (pd.DataFrame): The DataFrame to create masks for.
3. **Returns:**
   - Tuple[Tuple[pd.Series, pd.Series], Tuple[pd.Series, pd.Series]]: A tuple containing masks for filtering based on category and market direction.

---
