# Signal Generation Project

## Overview
This project is designed to generate trading signals based on input data. It includes modules for reading data, processing trades, and validating inputs.

## File Structure

- `source/`
  - `__init__.py`: Initialization file for the source package.
  - `constants.py`: Contains constant values used throughout the project.
  - `data_reader.py`: Reads and processes input data files.
  - `streamlit.py`: Streamlit app for getting user inputs.
  - `trade_processor.py`: Processes trades based on the input data.
  - `trade.py`: Handles trade logic and calculations.
  - `validations.py`: Validates input data and processed trade data.
- `venv/`: Virtual environment for the project.
- `.env`: Environment variables file.
- `.gitignore`: Git ignore file to exclude certain files from version control.
- `requirements.txt`: Python dependencies required for the project.
- `user_inputs.csv`: Example input data file for the project.

## Setup

1. Clone the repository:
    ```bash
    git clone <repository-url>
    ```
2. Navigate to the project directory:
    ```bash
    cd signal_generation
    ```
3. Create a virtual environment:
    ```bash
    python3 -m venv venv
    ```
4. Activate the virtual environment:
    - On Windows:
        ```bash
        venv\Scripts\activate
        ```
    - On macOS/Linux:
        ```bash
        source venv/bin/activate
        ```
5. Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. To run the Streamlit app:
    ```bash
    streamlit run source/streamlit.py
    ```
2. To process trades:
    ```bash
    python source/trade_processor.py
    ```

## Contributing
Contributions are welcome! Please create a pull request with your changes.
