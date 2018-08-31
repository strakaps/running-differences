from collections import deque


def get_window_length(window_path):
    with open(window_path, 'r') as fo:
        return int(fo.readline())


def get_starting_hour(input_path):
    """
    Returns the beginning hour.
    Params:
        input_path: path to either actual or predicted data
    """

    with open(input_path) as fo:
        hour = int(fo.readline().split('|')[0])
    return hour

def price_by_hour_generator(input_path):
    """
    Returns a generator of pairs (hour, prices) where hour is
    the current hour and prices is a dictionary
    of stock prices.  Assumption is that hour is
    increasing.
    Params:
        input_path: path to either actual or predicted data
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
    # output of last hour
    yield (current_hour, stockprices)


def errors_by_hour_generator(actual_generator, predicted_generator):
    """
    Returns a generator yielding tuples (hour, error_list)
    """

    # advance both actual and predicted data by one hour
    for hour1, actual in actual_generator:
        try:
            hour2, predicted = next(predicted_generator)
        except StopIteration:
            print("Ran out of predicted prices.")
        # if one of the two generators skips an hour,
        # let the other catch up
        while (hour1 != hour2):
            if hour1 < hour2:
                print("Actual data skipped one hour")
                hour1, actual = next(actual_generator)
            else:
                print("Predicted data skipped one hour")
                hour2, predicted = next(predicted_generator)
        yield hour1, errors_by_hour(actual, predicted)


def errors_by_hour(actual, predicted):
    """
    Compares the two stockprice dictionaries 'actual' and 'predicted'
    and returns a list of absolute price differences
    """

    errors = []
    for stock in predicted:
        if stock in actual:
            errors.append(abs(predicted[stock]-actual[stock]))
    return errors


def mean_error_by_window(actual_generator, predicted_generator, window_length):
    # tracks the most recent 'window_length' prices:
    error_window = deque(maxlen=window_length)
    # tracks the hours corresponding to the prices:
    hours = deque(maxlen=window_length)
    # fill queues
    while (len(error_window) < window_length):
        hour, errors = next(errors_by_hour_generator(actual_generator, predicted_generator))
        error_window.append(errors)
        hours.append(hour)
    # return first average error
    yield hours[0], hours[0] + window_length - 1, get_mean(error_window, hours)
    for hour, errors in errors_by_hour_generator(actual_generator, predicted_generator):
        # update errors and hours
        error_window.popleft()
        error_window.append(errors)
        hours.popleft()
        hours.append(hour)
        yield hours[0], hours[0] + window_length - 1, get_mean(error_window, hours)


def get_mean(error_window, hours):
    """
    Given a list of error lists and a corresponding list of hours,
    return the error averaged over the first `window_length` hours.
    """

    window_length = len(error_window)
    start_hour = hours[0]
    total = 0 # sum of prices
    tally = 0 # number of records
    for k in range(window_length):
        # to prevent the case where there are skipped hours:
        if (hours[k] - start_hour >= window_length):
            break
        total += sum(error_window[k])
        tally += len(error_window[k])
    return float(total / tally)


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

    predicted_generator = price_by_hour_generator(predicted_path)
    actual_generator = price_by_hour_generator(actual_path)
    error_generator = mean_error_by_window(actual_generator, predicted_generator, window_length)

    write_output(out_path)
