import csv
import os


def make_positive(value):
    if value < 0:
        return value * -1
    return value


def make_round(value):
    return round(value, 2)


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
