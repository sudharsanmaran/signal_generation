from collections import defaultdict
from typing import List, Tuple


def categorize_signal(all_combinations: List[Tuple[str, ...]]) -> int:
    """Categorize a signal based on mismatches with all combinations."""

    categories = defaultdict(set)
    length = len(all_combinations[0])
    while length:
        for combination in all_combinations:
            if any(combination.count(i) == length for i in combination):
                if combination not in categories[length + 1]:
                    categories[length].add(combination)
        length -= 1

    # remove empty categories
    cat_to_remove = [cat for cat in categories if len(categories[cat]) <= 1]
    for cat in cat_to_remove:
        del categories[cat]

    return categories
