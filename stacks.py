import os
import sqlite3
import sys
import re

import pandas as pd


def get_db_data(db_path):
    """
    Prints the table schema
    :param db_path: Path to the sqlite3 file
    :return pandas table containing the data in the db table
    """
    db = sqlite3.connect(db_path)
    pd.options.display.max_colwidth = 1000
    return pd.read_sql_query("SELECT * from outliers", db)


def print_table_schema(table):
    """
    Prints the table schema
    :param table: A pandas table containing the data in the db table
    """
    columns = list(table)
    print('Schema of the outliers table is:')
    print(columns)


def count_endpoint_outliers(table):
    """
    Prints the the number of outliers per endpoint.
    :param table: A pandas table containing the data in the db table
    """
    endpoints = {}
    for endpoint in table['endpoint']:
        endpoints[endpoint] = endpoints.get(endpoint, 0) + 1
    for k, v in endpoints.items():
        print('%s : %d' % (k, v))


def remove_multiple_strings(cur_string, replace_list):
    for cur_word in replace_list:
        cur_string = cur_string.replace(cur_word, '')
    return cur_string


def split_calls_regex(stacktrace, regex):
    return re.split(regex, stacktrace)


def clean_file(file_path):
    try:
        os.remove(file_path)
    except OSError:
        pass


def get_calls_frequency(all_stacks):
    calls_dict = {}
    for stack in all_stacks:
        for idx, call in enumerate(stack):
            calls_dict[call] = calls_dict.get(call, 0) + 1

    for call in sorted(calls_dict, key=calls_dict.get, reverse=True):
        print('%s : %d' % (call, calls_dict[call]))


def get_tuple_stack_element(stack_element):
    """
    :param stack_element: A string in the form:
        "File "/usr/local/lib/python3.6/threading.py", line 884, in _bootstrap    self._bootstrap_inner() "
    :return tuple of the form (/usr/local/lib/python3.6/threading.py, 884, _bootstrap, self._bootstrap_inner())
    """
    split_0 = stack_element.split('"', 1)
    # The line containing the thread id has the form ": 1234324235" or "(Thread-1138, started daemon 139968529811200)>"
    if len(split_0) == 1:
        return ['', '', '', '']

    after_file = split_0[1]
    split_1 = after_file.split(',', 1)
    file_name = split_1[0].strip().replace('"', '')

    split_2 = split_1[1].split(',', 1)
    line_number = split_2[0].replace('line', '').strip()

    split_3 = split_2[1].split('  ', 1)
    method_name = split_3[0].replace('in', '').strip()

    line_text = ''
    # check the case of lambda functions
    if len(split_3) > 1:
        line_text = split_3[1].strip()

    return [file_name, line_number, method_name, line_text]


def parse_stacktrace(stacktrace):
    """
    :param stacktrace: The full stacktrace of an outlier
    :return list of tuples representing the stacktrace, with the form
     (/usr/local/lib/python3.6/threading.py, 884, _bootstrap, self._bootstrap_inner()). Threads in the same stacktrace
     are separated by an empty tuple.
    """
    tuples_list = []
    for line in stacktrace:
        if line == '' or line == 'File' or line[0] == '<' or line == '# Thread_id':
            continue

        tuple_line = get_tuple_stack_element(line)
        if tuple_line is not None:
            tuples_list.append(tuple_line)
    return tuples_list


def get_dict_line_count(all_stacks_tuples):
    """
    :param all_stacks_tuples: All the stacktraces of an endpoint, as a list of lists of tuples
    :return dictionary where key is a tuple and value is the count
    """
    dict_line_count = {}
    for stack in all_stacks_tuples:
        for t in stack:
            if t == ['', '', '', '']:
                continue
            fn_ln = t[0] + ":" + t[1]
            dict_line_count[fn_ln] = dict_line_count.get(fn_ln, 0) + 1

    for t in sorted(dict_line_count, key=dict_line_count.get, reverse=True):
        print('%s : %d' % (t, dict_line_count[t]))

    return dict_line_count


def main():
    db_path = 'flask-dashboard.db'
    file_path = 'temp.txt'
    endpoint = 'api.get_possible_translations'
    replace_list = ['<br />', '\n', '\r']
    regex = '(File|# Thread_id|<Thread)'

    table = get_db_data(db_path)

    print_table_schema(table)
    count_endpoint_outliers(table)

    clean_file(file_path)

    all_stacks = []

    for index, row in table.iterrows():
        if row['endpoint'] == endpoint:
            if row['id'] == 13:
                print(row['stacktrace'])
            with open(os.path.join(sys.path[0], file_path), 'a') as writeFile:
                stacktrace_clean = remove_multiple_strings(row['stacktrace'], replace_list)
                writeFile.write('%s, %s\n' % (row['id'], row['endpoint']))
                calls = split_calls_regex(stacktrace_clean, regex)
                all_stacks.append(calls)
                for call in calls:
                    writeFile.write('%s\n' % call)
                writeFile.write('\n\n')

    all_stacks_tuples = []
    for stack in all_stacks:
        tuples_list = parse_stacktrace(stack)
        all_stacks_tuples.append(tuples_list)
        for t in tuples_list:
            print(t)
        print('\n\n\n')

    get_dict_line_count(all_stacks_tuples)


if __name__ == "__main__":
    main()
