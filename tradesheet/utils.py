import pandas as pd
def clean_int(value):
    try:
        value = int(value.replace(",", "")) if isinstance(value, str) else value
    except:
        pass
    return value


def percentage(value, percent):
   try:
       if isinstance(value, str):
           value = int(value.replace(",", ""))
       return (value * percent)/100
   except:
       return None


def int_to_roman(num):
    # Define a list of tuples containing the integer values and their corresponding Roman numeral symbols
    val = [
        (1000, 'M'), (900, 'CM'), (500, 'D'), (400, 'CD'),
        (100, 'C'), (90, 'XC'), (50, 'L'), (40, 'XL'),
        (10, 'X'), (9, 'IX'), (5, 'V'), (4, 'IV'), (1, 'I')
    ]

    # Initialize the result string
    roman_numeral = ''

    # Iterate through the values and symbols, constructing the Roman numeral
    for (value, symbol) in val:
        while num >= value:
            roman_numeral += symbol
            num -= value

    return roman_numeral


def get_bool(val):
    true = ['true', 't', '1', 'y', 'yes', 'enabled', 'enable', 'on']
    false = ['false', 'f', '0', 'n', 'no', 'disabled', 'disable', 'off', '', None]

    if isinstance(val, bool):
        return val
    if val.lower() in true:
        return True
    elif val.lower() in false:
        return False
    else:
        raise ValueError('The value \'{}\' cannot be mapped to boolean.'
                         .format(val))

