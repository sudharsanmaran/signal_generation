## Function: update_signal_start_duration

1. **Purpose:** Updates the duration from the start of a signal.
2. **Formula:**
   - Duration = Round((First entry `dt` in `cycle_data` - `dt` in `group_start_row`).total_seconds())
   - Formatted Duration = Format(Duration)

## Function: update_move

1. **Purpose:** Updates the move value based on cycle data.
2. **Formula:**
   - Move Value = Round(First entry `Close` in `cycle_data` - `Close` in `group_start_row`)
   - Positive Move Value = Make Positive(Move Value)

## Function: update_move_percent

1. **Purpose:** Updates the percentage move based on the move value.
2. **Formula:**
   - Percentage Move = Round((Move Value / `Close` in `group_start_row`) \* 100)
   - Positive Percentage Move = Make Positive(Percentage Move)

## Function: update_cycle_duration

1. **Purpose:** Updates the duration of the cycle.
2. **Formula:**
   - Duration = Round((Last entry `dt` in `cycle_data` - First entry `dt` in `cycle_data`).total_seconds())
   - Formatted Duration = Format(Duration)

## Function: update_cycle_min_max

1. **Purpose:** Updates the minimum and maximum values of the cycle.
2. **Formula:**
   - If `max_idx` is provided:
     - Maximum Value = `High` at `max_idx` in `adjusted_cycle_data`
   - Else:
     - Maximum Value = `cycle_max`
   - If `min_idx` is provided:
     - Minimum Value = `Low` at `min_idx` in `adjusted_cycle_data`
   - Else:
     - Minimum Value = `cycle_min`

## Function: update_max_to_min

1. **Purpose:** Updates the difference between the maximum and minimum values of the cycle.
2. **Formula:**
   - If `max_key` or `min_key` is None:
     - Difference = 0
   - Else:
     - Difference = Round(Maximum Value - Minimum Value)
   - If `is_last_cycle`:
     - Difference \*= -1

## Function: update_close_to_close

1. **Purpose:** Updates the difference between the closing values of the cycle.
2. **Formula:**
   - If `market_direction` is LONG:
     - Closing Difference = Round(Last Closing Value - First Closing Value)
   - Else:
     - Closing Difference = Round(First Closing Value - Last Closing Value)

## Function: update_avg_till_min_max

1. **Purpose:** Updates the average value until the minimum or maximum of the cycle is reached.
2. **Formula:**
   - If `market_direction` is LONG:
     - Average Low until Minimum = Round(Mean of `Low` values from start to `min_idx` in `adjusted_cycle_data`)
     - Average Max = Not Applicable
   - Else:
     - Average High until Maximum = Round(Mean of `High` values from start to `max_idx` in `adjusted_cycle_data`)
     - Average Min = Not Applicable

## Function: update_pnts_frm_avg_till_max_to_min

1. **Purpose:** Updates the points from the average value till the maximum or minimum value of the cycle.
2. **Formula:**
   - If `max_key` or `min_key` is None:
     - Points = Not Applicable
   - Else:
     - If `market_direction` is LONG:
       - Points = Round(Maximum Value - Average Low until Minimum)
     - Else:
       - Points = Round(Average High until Maximum - Minimum Value)

## Function: calculate_z_score

1. **Purpose:** Calculates the Z-score for a given DataFrame.
2. **Formula:**
   - Z-score = Round\[
     \frac{\text{Trailing 365 Days Return} - \text{Cumulative Average of Trailing 365 Days Return}}{\text{Cumulative Standard Deviation of Trailing 365 Days Return}}
     \]
3. **Inputs:**
   - `df`: A DataFrame containing the required columns.
4. **Outputs:**
   - Updates the DataFrame with a new column for the Z-score.

## Helper Function: update_cumulative_standard_dev

1. **Purpose:** Updates the DataFrame with the cumulative standard deviation for a specified return column.
2. **Formula:**
   - Cumulative Standard Deviation = Expanding Standard Deviation of the specified return column.
3. **Inputs:**
   - `df`: The DataFrame to update.
   - `key`: The column to calculate the standard deviation for (default is "Trailing 365 Days Return").
4. **Outputs:**
   - Adds a new column for the cumulative standard deviation in the DataFrame.

## Helper Function: update_cumulative_avg

1. **Purpose:** Updates the DataFrame with cumulative averages for specified columns.
2. **Formula:**
   - Cumulative Average = Expanding Mean of the specified columns.
3. **Inputs:**
   - `df`: The DataFrame to update.
   - `cols`: A list of columns for which to calculate the cumulative average.
4. **Outputs:**
   - Adds new columns for the cumulative averages in the DataFrame.

---

## Logic for Calculating CTC Rolling Averages

1. **Rolling Average Definition**: A rolling average is the average of a specific set of data points over a defined time period, which moves or "rolls" as new data points are added.

2. **Data Preparation**:

   - **Copy the DataFrame**: A copy of the original DataFrame is created to prevent changes to the original data during calculations.
   - **Set Index**: The index of the DataFrame is set to the "dt" (date/time) column to enable time-based rolling calculations.

3. **Rolling Average Calculation**:

   - For each specified rolling average (e.g., 90 days or 180 days):
     - **Time Frame Adjustment**: The rolling period is adjusted by subtracting a specified time frame (in minutes) from the defined rolling average duration.
     - **Calculate Mean**: The mean of the "Close to Close" values is calculated over the adjusted time frame using a rolling window.

4. **Update Original Data**: The calculated rolling averages are then stored back in the original DataFrame under the specified column names.

---
