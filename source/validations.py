# use for validations
def validate_input(
    instrument,
    strategy_id,
    start_date,
    end_date,
    fractal_file_number,
    bb_file_number,
    bb_band_sd,
):
    """
    1. Instument should be a string
    2. Strategy ID should be a string, should contain the strategy IDs separated by a comma and multiple conditions separated by a pipe
    3. Start date should be a string in the format "dd/mm/yyyy hh:mm:ss"
    4. End date should be a string in the format "dd/mm/yyyy hh:mm:ss"
    5. Fractal file number should be a string
    6. BB file number should be a string
    7. BB band standard deviation should be one of the following: 2.0, 2.25, 2.5, 2.75, 3.0
    8. Trail BB band standard deviation should be one of the following: 2.0, 2.25, 2.5, 2.75, 3.0
    9. BB band column should be one of the following: "mean", "upper", "lower"
    10. Trail BB band column should be one of the following: "mean", "upper", "lower"
    11. Trade start time should be a string in the format "hh:mm:ss"
    12. Trade end time should be a string in the format "hh:mm:ss"
    13. Check fractal should be a boolean
    14. Check BB band should be a boolean
    15. Check trail BB band should be a boolean
    16. Trail BB band direction should be one of the following: "higher", "lower"
    17. Trade type should be one of the following: "Intraday", "Positional"
    18. Allowed direction should be one of the following: "long", "short", "all"
    19. Fractal exit count should be an integer or "all"
    20. Long-short and entry-exit signals should be a string, should contain the signals separated by a comma and multiple conditions separated by a pipe
    21. Portfolio IDs should be a string, should contain the portfolio IDs separated by a comma
    22. count of strategy IDs should be equal to the count of portfolio IDs, irrespective of the number of conditions
    23. long entry singals should added in short exit signals
    24. short entry signals should added in long exit signals
    """
    pass