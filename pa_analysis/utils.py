import csv
import os


def make_positive(value):
    if value < 0:
        return value * -1
    return value


def make_round(value):
    return round(value, 2)


def format_duration(seconds):
    days = seconds / (3600 * 24)
    if days >= 1:
        return f"{make_round(days)} days"
    hours = seconds / 3600
    if hours >= 1:
        return f"{make_round(hours)} hours"
    minutes = seconds / 60
    return f"{make_round(minutes)} minutes"


def write_dict_to_csv(
    data,
    main_header,
    sub_header=None,
    output_dir="pa_analysis_output",
    csv_filename="final_result.csv",
):
    csv_file_path = os.path.join(output_dir, csv_filename)
    os.makedirs(output_dir, exist_ok=True)

    # Write to CSV
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)

        # Write main and sub headers
        writer.writerow(main_header)
        if sub_header:
            writer.writerow(sub_header)

        # Write the data rows
        for row in data:
            writer.writerow(row.values())
