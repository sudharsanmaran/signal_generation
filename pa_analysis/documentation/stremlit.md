# Streamlit Application Documentation

## Overview

This Streamlit application provides a user interface for conducting Portfolio Analysis (PA) and summarizing results. Users can select analysis options, input parameters, and view results dynamically. The application supports two modes:

1. **Single Analysis**: For conducting individual portfolio analysis.
2. **Summary**: For summarizing multiple analysis results.

## Requirements

- Python packages: `pathlib`, `time`, `pandas`, `streamlit`, `dotenv`
- Custom modules: `pa_analysis` and `source`

## Modules and Functions

### Imports

- **Path**: From `pathlib`, used for file system paths.
- **time**: Used for measuring execution time.
- **pandas**: Used for handling data structures and data analysis.
- **streamlit**: Used for creating the interactive web application.
- **load_dotenv**: From `dotenv`, used to load environment variables.
- **process**: From `pa_analysis.analysis_processor`, processes the analysis.
- **process_summaries**: From `pa_analysis.summary`, processes summary reports.
- **categorize_signal**: From `pa_analysis.utils`, categorizes signal combinations.
- **constants**: Various constants related to file paths and market directions.
- **streamlit utilities**: Functions for setting and validating user inputs.
- **validate**: From `pa_analysis.validation`, validates user inputs.

### Main Function

#### `main()`

The main function serves as the entry point for the Streamlit app. It provides the user interface and controls the flow of the application based on user input.

#### Key Components:

1. **Expander Selection**:

   - Users can select between "Single Analysis" and "Summary" modes using a select box.

2. **Single Analysis Mode**:

   - **Saved Inputs**: Users can choose to load saved inputs and filter saved notes.
   - **Include Volatile**: Checkbox to include volatile files with options to select files and tags.
   - **Include Volume**: Checkbox to include volume files with options to select files and tags.
   - **Portfolio IDs**: Inputs for specifying portfolio IDs.
   - **Direction and Instrument**: Inputs to set allowed directions and instruments.
   - **Entry and Exit Signals**: Options to define entry and exit signals.
   - **Additional Strategy File**: Checkbox for additional strategy file inputs.
   - **Date Range and Cycle Configs**: Set start and end dates, and cycle configurations.
   - **Submit Button**: Submits the form for processing, validates input, and processes results.

3. **Summary Mode**:
   - **File Selection**: Users can select multiple volatile output files to generate summaries.
   - **Submit Button**: Submits the selected files for summarization.

### Helper Functions

#### `set_entry_exit_signals_1(streamlit_inputs, saved_inputs, portfolio_ids, possible_flags_per_portfolio)`

This function helps set the entry and exit signals by:

- Filtering flag combinations based on portfolio IDs.
- Categorizing signals and allowing users to select long and short entry and exit signals based on the allowed market direction.

#### `set_entry_exit_signals(streamlit_inputs, saved_inputs, portfolio_ids, possible_flags_per_portfolio)`

This function is used if categorization is not needed. It sets entry and exit signals similarly to `set_entry_exit_signals_1` but without the categorization step.

## Environment Variables

The application loads environment variables using `dotenv`. Ensure the `.env` file is correctly set up for any required configurations.

## Error Handling

The application includes basic error handling:

- Displays errors if validation fails during submission.
- Notifies users if saved data is not found.

## Running the Application

To run the Streamlit application:

1. Ensure all dependencies are installed.
2. Run the script with Streamlit:
   ```bash
   streamlit run <your_script_name>.py
   ```

## Notes

- Ensure the paths defined in `source.constants` are correctly set up for the application to find input and output files.
- The application assumes that `pa_analysis` and `source` modules are correctly implemented and available in the Python path.

---
