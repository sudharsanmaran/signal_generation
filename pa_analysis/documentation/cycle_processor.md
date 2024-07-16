The `cycle_analysis_processor.py` script is responsible for processing trading cycles, calculating various metrics, and outputting the results to CSV files. Here's a detailed documentation of the file:

### Imports

python
from datetime import timedelta
from typing import List
import pandas as pd
from pandas.tseries.offsets import BDay

from pa_analysis.constants import (
MTMCrossedCycleColumns,
)
from source.processors.cycle_analysis_processor import (
adj_close_max_to_min,
get_min_max_idx,
get_next_cycle_first_row,
update_close_to_close,
update_cycle_min_max,
update_max_to_min,
)
from source.processors.cycle_trade_processor import (
get_cycle_base_df,
get_fractal_count_columns,
get_fractal_cycle_columns,
update_fractal_counter,
update_fractal_counter_1,
update_fractal_cycle_id,
update_second_cycle_id,
)
from source.utils import (
format_duration,
make_positive,
make_round,
write_dict_to_csv,
)
from source.constants import (
PA_ANALYSIS_CYCLE_FOLDER,
FirstCycleColumns,
MarketDirection,
SecondCycleIDColumns,
)
from source.processors.signal_trade_processor import write_dataframe_to_csv

These imports bring in necessary constants, functions, and classes from various modules.

### Function Definitions

#### `update_growth_percent_fractal_count(df, kwargs)`

Updates the growth percent for the fractal count in the DataFrame.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `kwargs`: A dictionary of additional arguments, including fractal standard deviation (`fractal_count_sd`).

#### `process_cycles(**kwargs)`

Processes the trading data to analyze cycles, calculate metrics, and write results to CSV files.

- **Parameters:**
  - `kwargs`: A dictionary of additional arguments needed for processing.

#### `update_max_to_min_percent(df, kwargs)`

Calculates the maximum to minimum percentage for cycles and updates the DataFrame with the results.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `kwargs`: A dictionary of additional arguments needed for processing.

#### `update_secondary_cycle_analytics(df, results, time_frame, prefix="CTC", cycle_count_col=SecondCycleIDColumns.CTC_CYCLE_ID.value, analytics_needed: List = [], positive_negative_keys: List = [])`

Updates secondary cycle analytics based on various metrics and writes results to the DataFrame.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `results`: List to store results.
  - `time_frame`: The time frame of the data.
  - `prefix`: Prefix for column names.
  - `cycle_count_col`: Column name for cycle count.
  - `analytics_needed`: List of analytics metrics needed.
  - `positive_negative_keys`: List of keys for positive/negative metrics.

#### `remove_cols(df, cols)`

Removes specified columns from the DataFrame.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `cols`: List of columns to be removed.

#### `update_pnts_frm_avg_till_max_to_min(**kwargs)`

Updates the points from average till maximum to minimum for a given cycle.

- **Parameters:**
  - `kwargs`: A dictionary of arguments needed for the calculation.

#### `update_avg_till_min_max(**kwargs)`

Updates the average till minimum and maximum values for a given cycle.

- **Parameters:**
  - `kwargs`: A dictionary of arguments needed for the calculation.

#### `analyze_cycles(df, time_frame, kwargs)`

Analyzes trading cycles in the DataFrame and calculates various metrics.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `time_frame`: The time frame of the data.
  - `kwargs`: A dictionary of additional arguments needed for processing.
- **Returns:** A list of dictionaries containing the results of the analysis.

### Helper Functions

#### `update_percent_with_grp_start_close(cycle_analysis, cols, group_start_close)`

Updates the percentage metrics with the group start close value.

- **Parameters:**
  - `cycle_analysis`: Dictionary containing cycle analysis results.
  - `cols`: List of columns to update.
  - `group_start_close`: The starting close value of the group.

#### `calculate_z_score(df)`

Calculates the Z-score for the trailing 365 days return.

- **Parameters:**
  - `df`: The DataFrame containing trading data.

#### `update_trail_return(df, trail_dates)`

Updates the trailing return in the DataFrame.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `trail_dates`: Dictionary of trailing date periods.

#### `prev_weekday(datetime)`

Gets the previous weekday for a given datetime.

- **Parameters:**

  - `datetime`: The datetime object.

- **Returns:** The previous weekday as a datetime object.

#### `update_trail_date_close(df, time_frame, trail_dates)`

Updates the trailing date close values in the DataFrame.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `time_frame`: The time frame of the data.
  - `trail_dates`: Dictionary of trailing date periods.

#### `update_trailling_date(df_copy, col_name, time_delta)`

Updates the trailing date in the DataFrame.

- **Parameters:**
  - `df_copy`: The copied DataFrame.
  - `col_name`: The column name to update.
  - `time_delta`: The time delta to calculate the trailing date.

#### `update_rolling_averages(df, time_frame, rolling_avg)`

Updates the rolling averages in the DataFrame.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `time_frame`: The time frame of the data.
  - `rolling_avg`: Dictionary of rolling average periods.

#### `update_rolling_avg_for_CTC(df, time_delta, col_name, min_periods=0)`

Updates the rolling average for Close-to-Close (CTC).

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `time_delta`: The time delta for the rolling average.
  - `col_name`: The column name to update.
  - `min_periods`: Minimum periods for rolling calculation.

#### `update_cumulative_avg(df, cols)`

Updates the cumulative average in the DataFrame.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `cols`: List of columns to update.

#### `update_positive_negative(**kwargs)`

Updates the positive or negative indicator for the specified columns.

- **Parameters:**
  - `kwargs`: Dictionary containing necessary arguments.

#### `update_cycle_duration(**kwargs)`

Updates the cycle duration in the cycle analysis.

- **Parameters:**
  - `kwargs`: Dictionary containing necessary arguments.

#### `update_signal_start_duration(**kwargs)`

Updates the signal start duration in the cycle analysis.

- **Parameters:**
  - `kwargs`: Dictionary containing necessary arguments.

#### `update_move(**kwargs)`

Updates the move metric in the cycle analysis.

- **Parameters:**
  - `kwargs`: Dictionary containing necessary arguments.

#### `update_move_percent(**kwargs)`

Updates the move percentage in the cycle analysis.

- **Parameters:**
  - `kwargs`: Dictionary containing necessary arguments.

#### `update_duration_above_BB(kwargs, market_direction, cycle_col, cycle_analysis, cycle_data)`

Updates the duration above Bollinger Bands (BB) in the cycle analysis.

- **Parameters:**
  - `kwargs`: Dictionary containing necessary arguments.
  - `market_direction`: The market direction.
  - `cycle_col`: The cycle column.
  - `cycle_analysis`: Dictionary containing cycle analysis results.
  - `cycle_data`: The DataFrame containing cycle data.

#### `update_cumulative_standard_dev(df, key)`

Calculates and updates the cumulative standard deviation in the DataFrame.

- **Parameters:**
  - `df`: The DataFrame containing trading data.
  - `key`: The column name for which to calculate the standard deviation.
