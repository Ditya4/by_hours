from os import path
from datetime import date
from datetime import timedelta
from _codecs import encode


class Streams:
    def __init__(self, index=None, switch_id=None, switch_name=None,
                 stream_id=None,
                 stream_name=None):
        self.index = index
        self.switch_id = switch_id
        self.switch_name = switch_name
        self.stream_id = stream_id
        self.stream_name = stream_name

    def __str__(self):
        '''
        we return a string which almost looks like a list with str value
        of every field in record
        '''
        list_of_values = [str(t) for name, t in self.__dict__.items()
                          if type(t).__name__ != "function" and
                          not name.startswith("__")]
        line_to_return = "[" + " , ".join(list_of_values) + "]"
        return line_to_return


class ByHours:
    def __init__(self, index=None, stat_id=None, switch_id=None,
                 switch_name=None,
                 rs_id=None, rs_name=None, cday=None,
                 h0=None, h1=None, h2=None,
                 h3=None, h4=None, h5=None,
                 h6=None, h7=None, h8=None,
                 h9=None, h10=None, h11=None,
                 h12=None, h13=None, h14=None,
                 h15=None, h16=None, h17=None,
                 h18=None, h19=None, h20=None,
                 h21=None, h22=None, h23=None):
        self.index = index
        self.stat_id = stat_id
        self.switch_id = switch_id
        self.switch_name = switch_name
        self.rs_id = rs_id
        self.rs_name = rs_name
        self.cday = cday
        self.h0 = h0
        self.h1 = h1
        self.h2 = h2
        self.h3 = h3
        self.h4 = h4
        self.h5 = h5
        self.h6 = h6
        self.h7 = h7
        self.h8 = h8
        self.h9 = h9
        self.h10 = h10
        self.h11 = h11
        self.h12 = h12
        self.h13 = h13
        self.h14 = h14
        self.h15 = h15
        self.h16 = h16
        self.h17 = h17
        self.h18 = h18
        self.h19 = h19
        self.h20 = h20
        self.h21 = h21
        self.h22 = h22
        self.h23 = h23
        # calculated fields
        self.date_date = None


    def __str__(self):
        '''
        we return a string which almost looks like a list with str value
        of every field in record
        '''
        list_of_values = [str(t) for name, t in self.__dict__.items()
                          if type(t).__name__ != "function" and
                          not name.startswith("__")]
        line_to_return = "[" + " , ".join(list_of_values) + "]"
        return line_to_return

    def date_to_date(self):
        '''
        we transform self.cday from string into date format and assign
        to "date_date"
        '''
        # print("we here", self.cday)
        self.date_date = to_date(self.cday)


'''
def read_by_hours(folder, file_name):
    by_hours_file_name = path.join(folder, file_name)
    by_hours_file = open(by_hours_file_name)
    by_hours_lines = by_hours_file.readlines()
    size_of_by_hours_list = len(by_hours_lines)
    for index in range(size_of_by_hours_list):
        by_hours_lines[index] = (
               by_hours_lines[index].rstrip())
    by_hours = [None] * size_of_by_hours_list
    in_by_hours_list_index = 0
    out_by_hours_list_index = 0
    while in_by_hours_list_index < size_of_by_hours_list:
        line_split = (
               by_hours_lines[in_by_hours_list_index].split(","))
        if line_split[-1] == "\n":
            line_split.pop()
        # print(in_by_hours_list_index, "line_split =", line_split)
        if len(line_split) == 30:
            by_hours[out_by_hours_list_index] = (
                            ByHours(out_by_hours_list_index,
                                    *line_split))
            in_by_hours_list_index += 1
            out_by_hours_list_index += 1
        else:
            print(f"Error in line from file = {file_name}",
                  f"with index = {in_by_hours_list_index}",
                  f"with value {line_split}",
                  f"wait for 30 parameters",
                  f"and got {len(line_split)}")
            size_of_by_hours_list -= 1
            in_by_hours_list_index += 1
            by_hours.pop()
    return by_hours
'''


def read_streams(folder, file_name):
    streams_file_name = path.join(folder, file_name)
    streams_file = open(streams_file_name)
    streams_lines = streams_file.readlines()
    size_of_streams_list = len(streams_lines)
    for index in range(size_of_streams_list):
        streams_lines[index] = (
               streams_lines[index].rstrip())
    streams = [None] * size_of_streams_list
    in_streams_list_index = 0
    out_streams_list_index = 0
    while in_streams_list_index < size_of_streams_list:
        line_split = (
               streams_lines[in_streams_list_index].split("    "))
        if line_split[-1] == "\n":
            line_split.pop()
        # print(in_streams_list_index, "line_split =", line_split)
        if len(line_split) == 4:
            streams[out_streams_list_index] = (
                            Streams(out_streams_list_index,
                                    *line_split))
            in_streams_list_index += 1
            out_streams_list_index += 1
        else:
            print(f"Error in line from file = {file_name}",
                  f"with index = {in_streams_list_index}",
                  f"with value {line_split}",
                  f"wait for 4 parameters",
                  f"and got {len(line_split)}")
            size_of_streams_list -= 1
            in_streams_list_index += 1
            streams.pop()
    return streams


def read_by_hours(folder, file_name):
    by_hours_file_name = path.join(folder, file_name)
    by_hours_file = open(by_hours_file_name, encoding="cp1251")
    by_hours_lines = by_hours_file.readlines()
    size_of_by_hours_list = len(by_hours_lines)
    for index in range(size_of_by_hours_list):
        by_hours_lines[index] = (
               by_hours_lines[index].rstrip())
    by_hours = [None] * size_of_by_hours_list
    in_by_hours_list_index = 0
    out_by_hours_list_index = 0
    while in_by_hours_list_index < size_of_by_hours_list:
        line_split = (
               by_hours_lines[in_by_hours_list_index].split(","))
        if line_split[-1] == "\n":
            line_split.pop()
        # print(in_by_hours_list_index, "line_split =", line_split)
        if len(line_split) == 30:
            by_hours[out_by_hours_list_index] = (
                            ByHours(out_by_hours_list_index,
                                    *line_split))
            in_by_hours_list_index += 1
            out_by_hours_list_index += 1
        else:
            print(f"Error in line from file = {file_name}",
                  f"with index = {in_by_hours_list_index}",
                  f"with value {line_split}",
                  f"wait for 30 parameters",
                  f"and got {len(line_split)}")
            size_of_by_hours_list -= 1
            in_by_hours_list_index += 1
            by_hours.pop()
    return by_hours


def to_date(str_date):
    date_to_return = date(year=get_year(str_date),
                          month=get_month(str_date),
                          day=get_day(str_date))
    return date_to_return


def get_dates_from_input_file(
        file_name='D:\python\double_dno\d_by_hours_uzh\input_dates.txt'):
    in_file = open(file_name, "r")
    date_start_str, date_end_str = in_file.read().split()
    start_date = to_date(date_start_str)
    end_date = to_date(date_end_str)
    return start_date, end_date


def get_year(str_date):
    '''
    we get
    01.01.2022
    10.01.2022
    date format dd.mm.yyyy
    '''
    year = str_date[6:]
    print("year =", year)

    # print(str_date("."))
    return int(year)


def get_month(str_date):
    '''
    we get
    01.01.2022
    10.01.2022
    date format dd.mm.yyyy
    '''
    month = str_date[3:5]
    print("month =", month)
    return int(month)


def get_day(str_date):
    '''
    we get
    01.01.2022
    10.01.2022
    date format dd.mm.yyyy
    '''
    day = str_date[:2]
    print("day =", day)
    return int(day)


def print_date(date_to_print):
    if date_to_print.day < 10:
        day_to_print = '0' + str(date_to_print.day)
        # print("day less than 10")
    else:
        day_to_print = date_to_print.day
    if date_to_print.month < 10:
        month_to_print = '0' + str(date_to_print.month)
        # print("month less than 10")
    else:
        month_to_print = date_to_print.month
    print(day_to_print, month_to_print, date_to_print.year, sep='.')
    pass


def find_zero_sequence(count_zero, by_hours_record, min_count_zero,
                       start_date, end_date):
    '''
    list_of_values = [str(t) for name, t in self.__dict__.items()
                          if type(t).__name__ != "function" and
                          not name.startswith("__")]
    line_to_return = "[" + " , ".join(list_of_values) + "]"
    '''
    # print(count_zero, by_hours_record, min_count_zero, start_date, end_date)
    list_of_values = [str(value) for name, value in
                      by_hours_record.__dict__.items()
                      if name.startswith("h")]
    list_of_names = [str(name) for name, value in
                     by_hours_record.__dict__.items()
                     if name.startswith("h")]
    # print(list_of_names)
    # print(type(list_of_names[0]))
    # print(list_of_values)
    # print(type(list_of_values[0]))
    for index in range(24):
        if int(list_of_values[index]) == 0:
            if count_zero >= min_count_zero and\
                        by_hours_record.date_date == end_date and\
                        index == 23:
                print("found more than", min_count_zero, "sequence on station",
                      by_hours_record.switch_id, "on flow",
                      by_hours_record.rs_id, "at date",
                      by_hours_record.cday, "end at hour", index)
                return 0
            count_zero += 1

        else:
            if count_zero >= min_count_zero:
                print("found more than", min_count_zero, "sequence on station",
                      by_hours_record.switch_id, "on flow",
                      by_hours_record.rs_id, "at date",
                      by_hours_record.cday, "end at hour", index)
                return 0
            count_zero = 0
    return count_zero


# main for by_hours part():
by_hours_folder = "D:\python\double_dno\d_by_hours_2_uzh"
by_hours_file_name = "output.csv"
list_of_by_hours = read_by_hours(by_hours_folder, by_hours_file_name)
print("by_hours_list:")
for record in list_of_by_hours:
    print(record)

# main for streams part():
streams_folder = "D:\python\double_dno\d_by_hours_2_uzh"
streams_file_name = "streams.txt"
list_of_streams = read_streams(streams_folder, streams_file_name)

min_count_zero = 24
print("streams_list:")
for record in list_of_streams:
    print(record)

print(list_of_by_hours[0])

if str(list_of_by_hours[0].rs_name) == ('"RS_NAME"'):
    start_line = 1
    print('file with headers')
else:
    start_line = 0
    print('file w/o headers')

print("by_hours_list:")
for index in range(start_line, len(list_of_by_hours)):
    list_of_by_hours[index].date_to_date()
    print(list_of_by_hours[index])
print()

start_date, end_date = get_dates_from_input_file(
    'D:\python\double_dno\d_by_hours_2_uzh\input_dates.txt')
print(start_date, end_date)
print_date(start_date)
print_date(end_date)
print(start_date.weekday(), end_date.weekday())

print_date(start_date + timedelta(days=1))
print_date(start_date + timedelta(days=2))

min_count_zero = 24
for stream_index in range(len(list_of_streams)):
    current_date = start_date
    count_zero = 0
    while current_date <= end_date:
        found_day = False
        for by_hours_index in range(len(list_of_by_hours)):
            if list_of_streams[stream_index].stream_id ==\
                        list_of_by_hours[by_hours_index].rs_id:
                if current_date == list_of_by_hours[by_hours_index].date_date:
                    found_day = True
        if found_day:
            pass
            count_zero = find_zero_sequence(count_zero,
                                            list_of_by_hours[by_hours_index],
                                            min_count_zero, start_date,
                                            end_date)

        else:
            print("Flow", list_of_streams[stream_index].stream_name,
                  "ID =", list_of_streams[stream_index].stream_id, "day",
                  current_date, "is empty")
            count_zero = 0

        current_date += timedelta(days=1)
