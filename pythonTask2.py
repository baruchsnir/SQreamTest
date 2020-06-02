# !/usr/bin/python
import psycopg2
import re
import json
from datetime import datetime

def get_delta_from_date(end_time, start_time):
    format = '%Y-%m-%d %H:%M:%S.%f'  # The format 2018-11-06 16:52:14.917
    datetime_end = datetime.strptime(end_time, format)
    datetime_start = datetime.strptime(start_time, format)
    milisecond = (datetime_end - datetime_start).microseconds/1000
    return milisecond
def collect_status_from_log():
    try:
        # r’id: (?P<id>\d+)’
        # (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\|\s*on thread\[\w* \w+\]\|\s*IP\[(\d+\.\d+\d+\.\d+\.\d+\:\d+)\]\s*\|\s*(\w+)\|\s*(\d+.\d+\d+.\d+.\d+)\|\s*(\w+)\|\s\(stmt : (\d+)\s*\)\s\|\s*(\w+)\|\s*(.*)
        rg = '''
        (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\|\s*on thread\[(\w*\s*\w+)\]\s*\|\s*IP\[(\d+\.\d+\d+\.\d+\.\d+\:\d+)\]\s*\|\s*(\w+)\|\s*(\d+.\d+\d+.\d+.\d+)\|\s*(\w+)\s*\|\s*\(stmt : (\d+)\s*\)\s*\|\s*(\w+)\|\s*(.*)
        '''

        with open('logfile.log','r') as file:
            lines = file.readlines()
        dd = re.findall(rg, lines)
        last_statement = ''
        data = {}
        statement = []
        Connection_id = -1
        pass_statement = ''
        user = ''
        start_time = ''
        end_time = ''
        total_time = ''
        for line in lines:
            lind_data = line.split('|')
            temp = lind_data[6]
            name = re.findall('stmt : (\d+)',lind_data[6])[0]
            if name != last_statement:
                if pass_statement != '':
                    data[last_statement]  = {'Connection_id' : Connection_id,'user': user,'status':pass_statement,'total_time': total_time}
                last_statement = name
                # Open New Record for new Statament
                statement = []
                Connection_id = -1
                pass_statement = ''
                user = ''
                start_time = ''
                end_time = ''
                total_time = 0
            if user == '':
                user = lind_data[5]
            status = str(lind_data[8])
            temp = status.lower().strip()
            # Get Connection ID only in the first time we read it
            if Connection_id == -1:
                if status.__contains__('Connection id'):
                    Connection_id = re.findall('Connection id - (\d+)',status)[0]
            # Get Start Time
            if start_time == '':
                if status.__contains__('Start Time'):
                    # start_time = re.findall('Start Time - (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})',status)[0]
                    start_time = lind_data[0]
            # Get End Time and calculate the total time for this statement
            if end_time == '':
                if status.__contains__('End Time'):
                    # end_time = re.findall('End Time - (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})',status)[0]
                    end_time = lind_data[0]
                    total_time = get_delta_from_date(end_time,start_time)
            # Get The Pass/Fail data
            if pass_statement == '':
                if temp == 'failed'  or temp == 'success':
                    pass_statement = temp
        # We add the last one
        if len(statement) > 0:
            data[last_statement]  = {'Connection_id' : Connection_id,'user': user,'status':pass_statement,'total_time': total_time}
        # Print Data to log for check it only in debug
        # print(data)
        # with open('new_ountrtries.js', "w") as f:
        #     json.dump(data,f,indent=2)
        return data
    except (Exception, psycopg2.DatabaseError) as ex:
        template = "Problem in collect_status_from_log - An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print('Exception - ', message)
    return ''


def count_succes_by_user(data):
    users = {}
    for s in data:
        if data[s]['status'] == 'success':
            name = data[s]['user']
            if name in users:
                users[name] += 1
            else:
                users[name] = 1
    print('------------------------------------------')
    print('Task 2 Number of Successful statements:')
    for x in  users:
        print('{} -  {}'.format(x,users[x]))
def count_succes_by_user_for_connection_id(data):
    conection_id = {}
    id = 0
    for s in data:
        if data[s]['status'] == 'success':
            id = data[s]['Connection_id']
            if id in conection_id:
                conection_id[id] += 1
            else:
                conection_id[id] = 1
    print('------------------------------------------')
    print('Task 2 Number of Successful statements for each Connection id :')
    for x in  conection_id:
        print('Connection Number {}:  {} Successful statements'.format(x,conection_id[x]))

def count_fail_sucess_sent_by_user(data):
    users = {}
    for s in data:
        name = data[s]['user']
        if name not in users:
            users[name] = {}
            users[name]['pass'] = 0
            users[name]['fail'] = 0
        if data[s]['status'] == 'success':
            users[name]['pass'] += 1
        else:
            users[name]['fail'] += 1
    print('------------------------------------------')
    print('Task 2 Number of Fail/Success statements for user:')
    for x in  users:
        print('-----------------')
        print('user {} sent {} successful statements'.format(x,users[x]['pass']))
        print('user {} sent {} Failed statements'.format(x,users[x]['fail']))
def get_slowest_successful_statement(data):
    statment = ''
    total = 0
    for s in data:
        if data[s]['total_time'] > total:
            total = data[s]['total_time']
            statment = s
    print('------------------------------------------')
    print('Task 2 Get the Slowest Statement :')
    print('statement {} was the slowest - {} Milliseconds'.format(statment,total))


def count_statements_by_user(data):
    users = {}
    for s in data:
        name = data[s]['user']
        if name in users:
            users[name] += 1
        else:
            users[name] = 1
    print('------------------------------------------')
    print('Task 2 Number of Statements were sent by User:')
    for x in  users:
        print('{} -  {}'.format(x,users[x]))

if __name__ == '__main__':
    data = collect_status_from_log()
    count_succes_by_user(data)
    count_succes_by_user_for_connection_id(data)
    count_fail_sucess_sent_by_user(data)
    get_slowest_successful_statement(data)
    count_statements_by_user(data)
