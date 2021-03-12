# This module should contain classes representing simple datatypes (like bytes or time)
# It might be helpful to be able to display these values as strings, in beautified format
# e.g. 10 min 34 s, instead of 634 s
# or   2.6 GB, instead of 2644123587 bytes
import re


def fmt_time(value):
    """
    < 60 seconds -> displayed in seconds (limit the decimal digits to 1 or keep int)
    < 3600 seconds -> display as X m Y s (if float, trim decimal digits)
    >= 3600 seconds -> display as X h Y m Z s (if float, trim decimal digits)
    :param value: seconds or None
    :return: Formatted value
    """
    if value is None:
        return "N/A"

    if not isinstance(value, int) and not isinstance(value, float):
        raise TypeError(f"Expected int or float in TimeDuration, got {type(value)}")

    if 0 < value < 0.1:
        return f"{value:.3f} s"
    elif value < 60:
        if isinstance(value, int):
            return f"{value} s"
        else:
            return f"{value:.1f} s"
    elif value < 3600:
        return f"{value//60:.0f} m {value%60:.0f} s"
    else:
        return f"{value//3600:.0f} h {(value%3600)//60:.0f} m {value%60:.0f} s"


def fmt_bytes(value):
    """
    < 1024 -> display as X B
    < 1024**2 -> display as X kB with one decimal digit
    < 1024**3 -> display as X MB with one decimal digit
    etc.
    :param value: bytes
    :return: formatted human readable value
    """
    if value is None:
        return "N/A"

    for unit in ['B', 'kB', 'MB', 'GB', 'TB']:
        if value < 1024:
            return f"{value:.{int(unit != 'B')}f} {unit}"
        value /= 1024.0
    return f"{value:.1f} PB"


def cast_or_none(value, cast_function):
    """
    Cast a value to a desired type, or return None if the cast is not possible
    :param value: value
    :param cast_function: cast function like float, int or str
    :return: casted value or None
    """
    try:
        return cast_function(value)
    except (TypeError, ValueError):
        return None


def size_in_bytes(size_str, default):
    """
    Read a (memory) size as string and convert it to bytes.
    :param size_str: memory size as string. Allowed formats like "1024", "1G", "1g", "1.0 G", "1.0 GB"
    :param default: a fallback value as int
    :return:
    """
    if size_str is None and isinstance(default, int):
        return default

    result = re.match(r"^(\d+(?:\.\d+)?)\ *[tT]B?$", size_str)
    if result:
        return int(float(result.group(1)) * (1 << 40))

    result = re.match(r"^(\d+(?:\.\d+)?)\ *[gG]B?$", size_str)
    if result:
        return int(float(result.group(1)) * (1 << 30))

    # spark interprets "2048" as 2 GB
    result = re.match(r"^(\d+(?:\.\d+)?)\ *[mM]?B?$", size_str)
    if result:
        return int(float(result.group(1)) * (1 << 20))

    result = re.match(r"^(\d+(?:\.\d+)?)\ *[kK]B?$", size_str)
    if result:
        return int(float(result.group(1)) * 1024)

    result = re.match(r"^(\d+) *B$", size_str)
    if result:
        return int(result.group(1))
    else:
        raise ValueError(f"Invalid size: {size_str}")


def cast_to_bool(string):
    """
    Cast string value to boolean.
    :param string: string
    :return: boolean or None if string is None
    """
    if string is None:
        return None

    values = {"true": True,
              "false": False,
              "1": True,
              "0": False,
              "yes": True,
              "no": False,
              }
    try:
        if string.lower() in values:
            return values[string.lower()]
        else:
            raise ValueError(f"Expected a string that can be converted to boolean, got {string}")
    except AttributeError as e:
        raise ValueError(f"Expected a string that can be converted to boolean, got {string}", e)


