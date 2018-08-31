from collections import deque
from functools import reduce


#predicted_path = "../insight_testsuite/tests/test_1/input/predicted.txt"
#actual_path = "../insight_testsuite/tests/test_1/input/actual.txt"
#window_path = "../insight_testsuite/tests/test_1/input/window.txt"
#out_path = "../output/comparison.txt"

def get_window_length(window_path):
    with open(window_path, 'r') as fo:
        return int(fo.readline())

def get_starting_hour(input_path):
    count = 0
    with open(input_path) as fo:
        hour = int(fo.readline().split('|')[0])
    return hour

def price_per_hour(input_path):
    """
    Returns a generator of tuples (hour, prices) where hour is
    the current hour and prices is a dictionary
    of stock prices.  Assumption is that hour is
    increasing.
    """

    stockprices = {}
    current_hour = get_starting_hour(input_path)
    with open(input_path, 'r') as fo:
        for line in fo:
            # unpack line
            triple = line.rstrip().split('|')
            hour = int(triple[0])
            stock = triple[1]
            value = float(triple[2])
            # check if end of hour
            if (hour > current_hour):
                yield (current_hour, stockprices)
                stockprices = {}
                current_hour = hour
            # fill dictionary
            stockprices[stock] = value
            # break if hour over
    yield (current_hour, stockprices)


def errors_per_hour(actual, predicted):
    errors = []
    for stock in predicted:
        if stock in actual:
            errors.append(abs(predicted[stock]-actual[stock]))
    return errors

def errors_per_hour_list(actual_generator, predicted_generator):
    """
    Returns a generator yielding tuples (hour, error_list)
    """

    for hour1, actual in actual_generator:
        try:
            hour2, predicted = next(predicted_generator)
        except StopIteration:
            print("No more predicted prices.")
        while (hour1 != hour2):
            print("Hours don't match...")
            if hour1 < hour2:
                hour1, actual = next(actual_generator)
            else:
                hour2, predicted = next(predicted_generator)
        yield hour1, errors_per_hour(actual, predicted)

def get_mean(error_window, hours):
    window_length = len(error_window)
    start_hour = hours[0]
    total = 0 # sum of prices
    tally = 0 # number of records
    for k in range(window_length):
        if (hours[k] - start_hour >= window_length):
            break
        total += sum(error_window[k])
        tally += len(error_window[k])
    return round(total / tally, 2)

def mean_error_per_window(actual_generator, predicted_generator, window_length):
    error_window = deque(maxlen=window_length)
    hours = deque(maxlen=window_length)
    # fill queues
    while (len(error_window) < window_length):
        hour, errors = next(errors_per_hour_list(actual_generator, predicted_generator))
        error_window.append(errors)
        hours.append(hour)
    yield hours[0], hours[-1], get_mean(error_window, hours)
    for hour, errors in errors_per_hour_list(actual_generator, predicted_generator):
        error_window.popleft()
        error_window.append(errors)
        hours.popleft()
        hours.append(hour)
        yield hours[0], hours[0]+window_length-1, get_mean(error_window, hours)

def write_output(out_path):
    with open(out_path, 'w') as fo:
        for triple in error_generator:
            line = str(triple[0]) + '|' + str(triple[1]) + '|' \
                    + "{:.2f}".format(triple[2])
            fo.write(line + '\n')



if __name__ == "__main__":
    import sys

    window_path = sys.argv[1]
    actual_path = sys.argv[2]
    predicted_path = sys.argv[3]
    out_path = sys.argv[4]

    window_length = get_window_length(window_path)

    predicted_generator = price_per_hour(predicted_path)
    actual_generator = price_per_hour(actual_path)
    error_generator = mean_error_per_window(actual_generator, predicted_generator, window_length)

    write_output(out_path)
