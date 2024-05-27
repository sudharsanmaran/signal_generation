### Signal Generation Project

## Overview

This project generates trading signals based on user inputs and market data. It utilizes a modular structure for data reading, trade processing, validation, and user interaction through a Streamlit app.

## File Structure

```
signal_generation/
├── source/
│   ├── __init__.py        # Initialization file for the source package
│   ├── constants.py       # Contains constant values used throughout the project
│   ├── data_reader.py     # Reads and processes input data files
│   ├── streamlit.py       # Streamlit app for getting user inputs
│   ├── trade_processor.py  # Processes trades based on the input data
│   └── trade.py           # Handles trade logic and calculations
│   └── validations.py    # Validates input data and processed trade data
├── requirements.txt      # Python dependencies required for the project
├── user_inputs.csv      # Example input data file for the project
├── .env                 # Optional environment variables file (if applicable)
├── .gitignore            # Git ignore file to exclude certain files from version control
└── venv/                 # Virtual environment for the project
```

## Setup

1. **Clone the Repository:**

   ```bash
   git clone <repository-url>
   ```

2. **Navigate to the Project Directory:**

   ```bash
   cd signal_generation
   ```

3. **Create and Activate the Virtual Environment:**

   - **Linux/macOS:**

     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```

   - **Windows:**

     ```bash
     python -m venv venv
     venv\Scripts\activate
     ```

4. **Install Required Packages:**

   ```bash
   pip install -r requirements.txt
   ```

## Setting Up the Python Path (Optional)

If you encounter module import issues, you might need to adjust your Python path to include the project's source directory. Here's how to do it for different operating systems:

**Linux/macOS:**

1. Open your terminal or shell.
2. Run the following command, replacing `<project_directory>` with the actual path to your project's root directory:

   ```bash
   export PYTHONPATH="${PYTHONPATH}:<project_directory>/source"
   ```

**Windows:**

- **CMD:**

  1. Open a command prompt window.
  2. Run the following command, replacing `<project_directory>` with the actual path to your project's root directory:

     ```bash
     set PYTHONPATH=%PYTHONPATH%;<project_directory>\source
     ```

- **PowerShell:**

  1. Open a PowerShell window.
  2. Run the following command, replacing `<project_directory>` with the actual path to your project's root directory:

     ```powershell
     $env:PYTHONPATH = "<project_directory>\source"; streamlit run .\source\streamlit.py
     ```

## Usage

1. **Run the Streamlit App (for User Interaction):**

   ```bash
   streamlit run source/streamlit.py
   ```

   This will launch the Streamlit app in your web browser, allowing you to input trade parameters and interact with the project.

## Contributing

We welcome contributions to this project! Please create a pull request outlining your changes.
