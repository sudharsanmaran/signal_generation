## Module Documentation: Volatile Analysis Summary Processor

This module processes summary files containing volatile analysis data and produces a summarized CSV file with various calculated metrics.

### Dependencies

```python
import pandas as pd
from source.constants import (
    VOLATILE_OUTPUT_FOLDER,
    VOLATILE_OUTPUT_SUMMARY_FOLDER,
)
from source.utils import make_round, write_dataframe_to_csv
from volatile_analysis.constants import (
    AnalysisColumn,
    AnalysisConstant,
    Operation,
    PosNegConstant,
    SummaryColumn,
    VolatileTag,
)
```

### Main Function

#### `process_summaries(files: list[str])`

Processes a list of summary files, calculates various metrics, and writes the result to a CSV file.

**Parameters:**

- `files` (list[str]): List of file names to be processed.

**Returns:**

- None

### Helper Functions

#### `get_multi_index() -> pd.MultiIndex`

Creates and returns a multi-level index for the resulting DataFrame.

**Returns:**

- `pd.MultiIndex`: Multi-level index.

#### `parse_file_terms(file: str) -> list[str]`

Parses a file name into its constituent terms.

**Parameters:**

- `file` (str): File name.

**Returns:**

- `list[str]`: List of parsed terms.

#### `parse_timeframes_and_periods(terms: list[str]) -> tuple`

Parses time frames and periods from the terms.

**Parameters:**

- `terms` (list[str]): List of parsed terms.

**Returns:**

- `tuple`: Tuple containing lists of time frames and periods.

#### `create_common_props(df: pd.DataFrame, terms: list[str]) -> dict`

Creates a dictionary of common properties extracted from the DataFrame and terms.

**Parameters:**

- `df` (pd.DataFrame): DataFrame containing the data.
- `terms` (list[str]): List of parsed terms.

**Returns:**

- `dict`: Dictionary of common properties.

#### `create_category_result(common_props: dict, idx: int) -> dict`

Creates a result dictionary for a specific category based on common properties and an index.

**Parameters:**

- `common_props` (dict): Dictionary of common properties.
- `idx` (int): Index to determine the category.

**Returns:**

- `dict`: Dictionary containing category-specific results.

#### `create_volatility_result(df: pd.DataFrame, adj_mask: pd.Series, cat_res: dict, volatila_tag_col: str) -> dict`

Creates a volatility result dictionary based on the DataFrame, adjusted mask, category results, and volatility tag column.

**Parameters:**

- `df` (pd.DataFrame): DataFrame containing the data.
- `adj_mask` (pd.Series): Adjusted mask series.
- `cat_res` (dict): Dictionary containing category-specific results.
- `volatila_tag_col` (str): Volatility tag column name.

**Returns:**

- `dict`: Dictionary containing volatility-specific results.

#### `process_summary(df: pd.DataFrame, file: str) -> list[dict]`

Processes a summary DataFrame and file, returning a list of result dictionaries.

**Parameters:**

- `df` (pd.DataFrame): DataFrame containing the data.
- `file` (str): File name.

**Returns:**

- `list[dict]`: List of result dictionaries.

#### `update_columns(df: pd.DataFrame, adj_mask: pd.Series, vol_res: dict, positive_mask: pd.Series, negative_mask: pd.Series)`

Updates the result dictionary with various column calculations based on the DataFrame and masks.

**Parameters:**

- `df` (pd.DataFrame): DataFrame containing the data.
- `adj_mask` (pd.Series): Adjusted mask series.
- `vol_res` (dict): Result dictionary.
- `positive_mask` (pd.Series): Mask for positive values.
- `negative_mask` (pd.Series): Mask for negative values.

#### `update_pos_neg_columns(df: pd.DataFrame, adj_mask: pd.Series, vol_res: dict, positive_mask: pd.Series, negative_mask: pd.Series, analysis_col: str, summary_col: str, operations: list)`

Updates the result dictionary with positive and negative column calculations.

**Parameters:**

- `df` (pd.DataFrame): DataFrame containing the data.
- `adj_mask` (pd.Series): Adjusted mask series.
- `vol_res` (dict): Result dictionary.
- `positive_mask` (pd.Series): Mask for positive values.
- `negative_mask` (pd.Series): Mask for negative values.
- `analysis_col` (str): Analysis column name.
- `summary_col` (str): Summary column name.
- `operations` (list): List of operations to perform.

#### `update_generic_columns(df: pd.DataFrame, adj_mask: pd.Series, vol_res: dict, sign: str, analysis_col: str, summary_col: str, operations: list)`

Updates the result dictionary with generic column calculations.

**Parameters:**

- `df` (pd.DataFrame): DataFrame containing the data.
- `adj_mask` (pd.Series): Adjusted mask series.
- `vol_res` (dict): Result dictionary.
- `sign` (str): Sign (positive or negative).
- `analysis_col` (str): Analysis column name.
- `summary_col` (str): Summary column name.
- `operations` (list): List of operations to perform.

#### `update_weighted_avg(sign: str, vol_res: dict, df: pd.DataFrame, adj_mask: pd.Series, col1: str, col2: str, new_col_name: str)`

Updates the result dictionary with weighted average calculations.

**Parameters:**

- `sign` (str): Sign (positive or negative).
- `vol_res` (dict): Result dictionary.
- `df` (pd.DataFrame): DataFrame containing the data.
- `adj_mask` (pd.Series): Adjusted mask series.
- `col1` (str): First column name.
- `col2` (str): Second column name.
- `new_col_name` (str): New column name.

#### `get_masks(df: pd.DataFrame, volatila_tag_col: str) -> tuple`

Returns various masks used in the processing.

**Parameters:**

- `df` (pd.DataFrame): DataFrame containing the data.
- `volatila_tag_col` (str): Volatility tag column name.

**Returns:**

- `tuple`: Tuple containing various masks.

#### `get_no_of_valid_cycles(df: pd.DataFrame, adj_mask: pd.Series) -> int`

Returns the number of valid cycles in the DataFrame.

**Parameters:**

- `df` (pd.DataFrame): DataFrame containing the data.
- `adj_mask` (pd.Series): Adjusted mask series.

**Returns:**

- `int`: Number of valid cycles.

#### `get_category(idx: int) -> str`

Returns the category based on the index.

**Parameters:**

- `idx` (int): Index to determine the category.

**Returns:**

- `str`: Category.

#### `read_files(files: list[str]) -> list[pd.DataFrame]`

Reads the files and returns a list of DataFrames.

**Parameters:**

- `files` (list[str]): List of file names.

**Returns:**

- `list[pd.DataFrame]`: List of DataFrames.
