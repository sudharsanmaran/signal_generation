import streamlit as st
import pandas as pd


def run_trading_strategy(row):

    print(f"Running strategy with parameters: {row.to_dict()}")

    pass


def main():
    st.title("Trading System Input")

    # File uploader
    uploaded_file = st.file_uploader(
        "Choose an Excel file with trading parameters", type="xlsx"
    )

    if uploaded_file:
        df = pd.read_excel(uploaded_file)

        # st.write("Uploaded Parameters:")
        # st.dataframe(df)

        if st.button("Run Strategies"):
            for index, row in df.iterrows():
                run_trading_strategy(row)
            st.success("All strategies have been executed.")


if __name__ == "__main__":
    main()
