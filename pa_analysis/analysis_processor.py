import os

from pa_analysis.constants import OutputHeader, RankingColumns, SignalColumns
from pa_analysis.cycle_processor import process_cycles
from source.constants import MarketDirection
from source.data_reader import load_strategy_data
from source.processors.cycle_trade_processor import get_base_df

from source.utils import (
    format_dates,
    make_positive,
    make_round,
    write_dict_to_csv,
)


# Function to flatten the nested dictionary
def flatten_dict(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def process(validated_data):
    strategy_pairs = validated_data.get("strategy_pairs", [])
    instruments = validated_data.get("instruments", [])
    result, data = {}, []
    for instrument in instruments:
        for strategy_pair in strategy_pairs:
            processed_result = process_strategy(
                validated_data, strategy_pair, instrument
            )
            temp = {"instrument": instrument, "strategy_pair": strategy_pair}
            result[(instrument, strategy_pair)] = processed_result
            temp.update(processed_result)
            data.append(temp)
    update_rankings(data)
    flattened_data, sub_header, collapsed_main_header = format(data)
    write_dict_to_csv(flattened_data, sub_header, collapsed_main_header)
    return result


def format(data):
    flattened_data = []
    for item in data:
        flat_data = flatten_dict(item)
        flattened_data.append(flat_data)

    # Get all keys for the header
    header = flattened_data[0].keys()

    # Create main and subheaders
    main_header = ["instrument", "strategy_pair"]
    sub_header = ["", ""]
    header_dict = {"instrument": [], "strategy_pair": []}

    for key in header:
        if key not in ["instrument", "strategy_pair"]:
            if "_" in key:
                main, sub = key.split("_", 1)
                if main not in header_dict:
                    header_dict[main] = []
                header_dict[main].append(sub)
            else:
                main_header.append(key)
                sub_header.append("")
                header_dict[key] = []
        else:
            header_dict[key] = []

    # Add main headers and their subheaders to main and sub_header lists
    for main in header_dict.keys():
        if main not in ["instrument", "strategy_pair"]:
            main_header.extend([main] * len(header_dict[main]))
            sub_header.extend(header_dict[main])

    # Collapse the main header by keeping only the first occurrence
    collapsed_main_header = []
    previous = None
    for header in main_header:
        if header != previous:
            collapsed_main_header.append(header)
            previous = header
        else:
            collapsed_main_header.append("")
    return flattened_data, sub_header, collapsed_main_header


def process_strategy(validated_data, strategy_pair, instrument):

    strategy_pair_str = "_".join(map(lambda x: str(x), strategy_pair))

    base_path = os.getenv("DB_PATH")

    start_datetime, end_datetime = format_dates(
        validated_data.get("start_date"), validated_data.get("end_date")
    )

    strategy_df = load_strategy_data(
        instrument,
        validated_data.get("portfolio_ids"),
        strategy_pair,
        start_datetime,
        end_datetime,
        base_path,
    )[0]

    base_df = get_base_df(
        validated_data, strategy_df, strategy_pair_str, instrument
    )

    # based on base df need to generate output analytic df
    result_base_df = generate_analytics(base_df)
    if validated_data["calculate_cycles"]:
        process_cycles(
            **validated_data,
            base_df=base_df,
            instrument=instrument,
        )
    return result_base_df


def generate_analytics(base_df) -> dict:
    """
    Generate the analytics for the strategy.

    Parameters:
        base_df (DataFrame): The base DataFrame containing the strategy data.

    Returns:
        dict: The dict containing the analytics for the strategy.
    """

    result = {
        OutputHeader.SIGNAL.value: {},
        OutputHeader.POINTS.value: {},
        OutputHeader.POINTS_PERCENT.value: {},
        OutputHeader.PROBABILITY.value: {},
        OutputHeader.POINTS_PER_SIGNAL.value: {},
        OutputHeader.POINTS_PER_SIGNAL_PERCENT.value: {},
        OutputHeader.RISK_REWARD.value: {},
        OutputHeader.SIGNAL_DURATION.value: {},
        OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value: {},
        OutputHeader.RANKING.value: {},
    }

    for direction in [MarketDirection.LONG, MarketDirection.SHORT]:
        mask_df, plus_mask_df, minus_mask_df = get_masked_df(
            base_df, direction
        )

        update_signals(
            result[OutputHeader.SIGNAL.value],
            direction,
            plus_mask_df,
            minus_mask_df,
        )

        update_points(
            result[OutputHeader.POINTS.value],
            direction,
            mask_df,
            plus_mask_df,
            minus_mask_df,
        )

        updated_points_percent(
            result[OutputHeader.POINTS_PERCENT.value],
            direction,
            mask_df,
            plus_mask_df,
            minus_mask_df,
        )

        update_probability(
            result[OutputHeader.PROBABILITY.value],
            direction,
            mask_df,
        )

        update_net_points_per_signal(
            result,
            direction,
        )

        update_net_points_per_signal_percent(
            result,
            direction,
        )

        update_risk_reward(result, direction)

        update_signal_duration(
            result[OutputHeader.SIGNAL_DURATION.value],
            direction,
            mask_df,
            plus_mask_df,
            minus_mask_df,
        )

        update_weighted_avg_signal_duration(
            result,
            direction,
            mask_df,
            plus_mask_df,
            minus_mask_df,
        )

    # Calculate totals
    update_totals(result, base_df)

    return result


def get_masked_df(base_df, direction):
    mask = base_df["market_direction"] == direction
    plus_mask = (base_df["points"] > 0) & mask
    minus_mask = (base_df["points"] < 0) & mask
    return base_df[mask], base_df[plus_mask], base_df[minus_mask]


def update_totals(result, base_df):
    result[OutputHeader.SIGNAL.value]["Total Signals"] = (
        result[OutputHeader.SIGNAL.value][SignalColumns.LONG_NET.value]
        + result[OutputHeader.SIGNAL.value][SignalColumns.SHORT_NET.value]
    )
    result[OutputHeader.POINTS.value]["Total Points"] = make_round(
        result[OutputHeader.POINTS.value][SignalColumns.LONG_NET.value]
        + result[OutputHeader.POINTS.value][SignalColumns.SHORT_NET.value]
    )
    result[OutputHeader.PROBABILITY.value]["Total"] = make_round(
        base_df["profit_loss"].mean() * 100
    )
    # result[OutputHeader.POINTS_PER_SIGNAL.value]["Total"] = make_round(
    #     result[OutputHeader.POINTS_PER_SIGNAL.value][
    #         SignalColumns.LONG_NET.value
    #     ]
    #     + result[OutputHeader.POINTS_PER_SIGNAL.value][
    #         SignalColumns.SHORT_NET.value
    #     ]
    # )
    result[OutputHeader.SIGNAL_DURATION.value]["Total"] = make_round(
        result[OutputHeader.SIGNAL_DURATION.value][
            SignalColumns.LONG_NET.value
        ]
        + result[OutputHeader.SIGNAL_DURATION.value][
            SignalColumns.SHORT_NET.value
        ]
    )
    # total_weight = base_df["temp"].sum() / base_df["points"].sum()
    # result[OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value]["Total"] = (
    #     make_round(make_positive_int(total_weight))
    # )

    result[OutputHeader.POINTS_PERCENT.value]["Total"] = make_round(
        result[OutputHeader.POINTS_PERCENT.value][SignalColumns.LONG_NET.value]
        + result[OutputHeader.POINTS_PERCENT.value][
            SignalColumns.SHORT_NET.value
        ]
    )

    result[OutputHeader.POINTS_PER_SIGNAL_PERCENT.value]["Total"] = make_round(
        result[OutputHeader.POINTS_PER_SIGNAL_PERCENT.value][
            SignalColumns.LONG_NET.value
        ]
        + result[OutputHeader.POINTS_PER_SIGNAL_PERCENT.value][
            SignalColumns.SHORT_NET.value
        ]
    )


def update_net_points_per_signal_percent(
    result,
    direction,
):
    plus, minus, net = get_col_name(direction)
    result[OutputHeader.POINTS_PER_SIGNAL_PERCENT.value][plus] = make_round(
        result[OutputHeader.POINTS_PERCENT.value][plus].mean()
    )
    result[OutputHeader.POINTS_PER_SIGNAL_PERCENT.value][minus] = make_round(
        result[OutputHeader.POINTS_PERCENT.value][minus].mean()
    )
    result[OutputHeader.POINTS_PER_SIGNAL_PERCENT.value][net] = make_round(
        result[OutputHeader.POINTS_PERCENT.value][net].mean()
    )


def updated_points_percent(
    result, direction, mask_df, plus_mask_df, minus_mask_df
):
    plus, minus, net = get_col_name(direction)
    result[plus] = make_round(plus_mask_df["points_percent"].sum())
    result[minus] = make_round(minus_mask_df["points_percent"].sum())
    result[net] = make_round(mask_df["points_percent"].sum())


def rank_nested_dict(data_dict, key, column_name, output_key="rank"):
    """
    Ranks elements in a nested dictionary and adds a new key with the rank based on a specified inner key.

    Args:
        data_dict: The nested dictionary to be ranked.
        key: The key in the inner dictionary to use for sorting and ranking.

    Returns:
        The modified dictionary with a new key named 'rank' added to each inner dictionary.
    """

    rank = 1
    sorted_data = sorted(data_dict, key=lambda item: item[key][column_name])
    for row in reversed(sorted_data):
        row[OutputHeader.RANKING.value][output_key] = rank
        rank += 1


def update_rankings(data):

    key_to_rank = {
        OutputHeader.PROBABILITY.value: [
            SignalColumns.LONG.value,
            SignalColumns.SHORT.value,
            "Total",
        ],
        OutputHeader.POINTS_PER_SIGNAL.value: [
            SignalColumns.LONG_PLUS.value,
            SignalColumns.SHORT_PLUS.value,
            # SignalColumns.LONG_MINUS.value,
            # SignalColumns.SHORT_MINUS.value,
        ],
        OutputHeader.RISK_REWARD.value: [
            SignalColumns.LONG_NET.value,
            SignalColumns.SHORT_NET.value,
        ],
        OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value: [
            SignalColumns.LONG_PLUS.value,
            SignalColumns.SHORT_PLUS.value,
            # SignalColumns.LONG_MINUS.value,
            # SignalColumns.SHORT_MINUS.value,
        ],
    }

    column_output_key = {
        (
            OutputHeader.PROBABILITY.value,
            SignalColumns.LONG.value,
        ): RankingColumns.PROBABILITY_LONG.value,
        (
            OutputHeader.PROBABILITY.value,
            SignalColumns.SHORT.value,
        ): RankingColumns.PROBABILITY_SHORT.value,
        (
            OutputHeader.PROBABILITY.value,
            "Total",
        ): RankingColumns.PROBABILITY_TOTAL.value,
        (
            OutputHeader.POINTS_PER_SIGNAL.value,
            SignalColumns.LONG_PLUS.value,
        ): RankingColumns.NET_POINTS_PER_SIGNAL_LONG_PLUS.value,
        (
            OutputHeader.POINTS_PER_SIGNAL.value,
            SignalColumns.SHORT_PLUS.value,
        ): RankingColumns.NET_POINTS_PER_SIGNAL_SHORT_PLUS.value,
        (
            OutputHeader.POINTS_PER_SIGNAL.value,
            SignalColumns.LONG_MINUS.value,
        ): RankingColumns.NET_POINTS_PER_SIGNAL_LONG_MINUS.value,
        (
            OutputHeader.POINTS_PER_SIGNAL.value,
            SignalColumns.SHORT_MINUS.value,
        ): RankingColumns.NET_POINTS_PER_SIGNAL_SHORT_MINUS.value,
        (
            OutputHeader.RISK_REWARD.value,
            SignalColumns.LONG_NET.value,
        ): RankingColumns.RISK_REWARD_LONG.value,
        (
            OutputHeader.RISK_REWARD.value,
            SignalColumns.SHORT_NET.value,
        ): RankingColumns.RISK_REWARD_SHORT.value,
        (
            OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value,
            SignalColumns.LONG_PLUS.value,
        ): RankingColumns.WEIGHTED_AVERAGE_SIGNAL_DURATION_LONG_PLUS.value,
        (
            OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value,
            SignalColumns.SHORT_PLUS.value,
        ): RankingColumns.WEIGHTED_AVERAGE_SIGNAL_DURATION_SHORT_PLUS.value,
        (
            OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value,
            SignalColumns.LONG_MINUS.value,
        ): RankingColumns.WEIGHTED_AVERAGE_SIGNAL_DURATION_LONG_MINUS.value,
        (
            OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value,
            SignalColumns.SHORT_MINUS.value,
        ): RankingColumns.WEIGHTED_AVERAGE_SIGNAL_DURATION_SHORT_MINUS.value,
    }

    for key, columns in key_to_rank.items():
        for col in columns:
            rank_nested_dict(
                data,
                key,
                column_name=col,
                output_key=column_output_key[(key, col)],
            )

    update_ranking_total(data)


def update_ranking_total(data):
    for val in data:
        val[OutputHeader.RANKING.value]["Total"] = (
            val[OutputHeader.RANKING.value][
                RankingColumns.PROBABILITY_LONG.value
            ]
            + val[OutputHeader.RANKING.value][
                RankingColumns.PROBABILITY_SHORT.value
            ]
            + val[OutputHeader.RANKING.value][
                RankingColumns.PROBABILITY_TOTAL.value
            ]
            + val[OutputHeader.RANKING.value][
                RankingColumns.NET_POINTS_PER_SIGNAL_LONG_PLUS.value
            ]
            # + val[OutputHeader.RANKING.value][
            #     RankingColumns.NET_POINTS_PER_SIGNAL_LONG_MINUS.value
            # ]
            + val[OutputHeader.RANKING.value][
                RankingColumns.NET_POINTS_PER_SIGNAL_SHORT_PLUS.value
            ]
            # + val[OutputHeader.RANKING.value][
            #     RankingColumns.NET_POINTS_PER_SIGNAL_SHORT_MINUS.value
            # ]
            + val[OutputHeader.RANKING.value][
                RankingColumns.RISK_REWARD_LONG.value
            ]
            + val[OutputHeader.RANKING.value][
                RankingColumns.RISK_REWARD_SHORT.value
            ]
            + val[OutputHeader.RANKING.value][
                RankingColumns.WEIGHTED_AVERAGE_SIGNAL_DURATION_LONG_PLUS.value
            ]
            # + val[OutputHeader.RANKING.value][
            #     RankingColumns.WEIGHTED_AVERAGE_SIGNAL_DURATION_LONG_MINUS.value
            # ]
            + val[OutputHeader.RANKING.value][
                RankingColumns.WEIGHTED_AVERAGE_SIGNAL_DURATION_SHORT_PLUS.value
            ]
            # + val[OutputHeader.RANKING.value][
            #     RankingColumns.WEIGHTED_AVERAGE_SIGNAL_DURATION_SHORT_MINUS.value
            # ]
        )


def update_weighted_avg_signal_duration(
    result, direction, mask_df, plus_mask_df, minus_mask_df
):
    plus, minus, net = get_col_name(direction)
    result[OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value][plus] = (
        make_round(
            make_positive(
                plus_mask_df["temp"].sum() / plus_mask_df["points"].sum()
            )
        )
    )
    result[OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value][minus] = (
        make_round(
            make_positive(
                minus_mask_df["temp"].sum() / minus_mask_df["points"].sum()
            )
        )
    )
    # result[OutputHeader.WEIGHTED_AVERAGE_SIGNAL_DURATION.value][net] = (
    #     make_round(
    #         make_positive_int(mask_df["temp"].sum() / mask_df["points"].sum())
    #     )
    # )


def update_signal_duration(
    result, direction, mask_df, plus_mask_df, minus_mask_df
):
    plus, minus, net = get_col_name(direction)
    result[plus] = make_round(plus_mask_df["time"].mean())
    result[minus] = make_round(minus_mask_df["time"].mean())
    result[net] = make_round(mask_df["time"].mean())


def update_risk_reward(result, direction):
    plus, minus, net = get_col_name(direction)
    result[OutputHeader.RISK_REWARD.value][net] = make_round(
        result[OutputHeader.POINTS_PER_SIGNAL.value][plus]
        / result[OutputHeader.POINTS_PER_SIGNAL.value][minus]
        * -1
    )


def update_net_points_per_signal(result, direction):
    plus, minus, net = get_col_name(direction)
    result[OutputHeader.POINTS_PER_SIGNAL.value][plus] = make_round(
        result[OutputHeader.POINTS.value][plus]
        / result[OutputHeader.SIGNAL.value][plus]
    )
    result[OutputHeader.POINTS_PER_SIGNAL.value][minus] = make_round(
        result[OutputHeader.POINTS.value][minus]
        / result[OutputHeader.SIGNAL.value][minus]
    )
    # result[OutputHeader.POINTS_PER_SIGNAL.value][net] = make_round(
    #     result[OutputHeader.POINTS.value][net]
    #     / result[OutputHeader.SIGNAL.value][net]
    # )


def update_probability(result, direction, mask_df):
    if direction == MarketDirection.LONG:
        col_name = SignalColumns.LONG.value
    else:
        col_name = SignalColumns.SHORT.value
    result[col_name] = make_round(mask_df["profit_loss"].mean() * 100)


def update_points(result, direction, mask_df, plus_mask_df, minus_mask_df):
    plus, minus, net = get_col_name(direction)
    result[plus] = make_round(plus_mask_df["points"].sum())
    result[minus] = make_round(minus_mask_df["points"].sum())
    result[net] = make_round(mask_df["points"].sum())


def get_col_name(direction):
    if direction == MarketDirection.LONG:
        plus, minus, net = (
            SignalColumns.LONG_PLUS.value,
            SignalColumns.LONG_MINUS.value,
            SignalColumns.LONG_NET.value,
        )
    else:
        plus, minus, net = (
            SignalColumns.SHORT_PLUS.value,
            SignalColumns.SHORT_MINUS.value,
            SignalColumns.SHORT_NET.value,
        )

    return plus, minus, net


def update_signals(result, direction, plus_mask_df, minus_mask_df):
    plus, minus, net = get_col_name(direction)
    # get total count of signals
    result[plus] = len(plus_mask_df)
    result[minus] = len(minus_mask_df)
    result[net] = result[plus] + result[minus]
