import csv
import pdb
import re
import datetime
import time
from dbfread import DBF, FieldParser


start_date = datetime.date(2000, 5, 25)
end_date = datetime.date(2017, 5, 26)


class MyFieldParser(FieldParser):
    def parse(self, field, data):
        try:
            if field.name == 'BRAHMS' or field.name == 'EDITHIST' or field.name == 'ID' or field.name == 'SPNUMBER':
                return FieldParser.parse(self, field, data)
            else:
                return None
        except ValueError:
            return InvalidValue(data)

            
class Record(dict):
    def __init__(self, items):
        for (name, value) in items:
            if name == 'BRAHMS' or name == 'EDITHIST' or name == 'ID' or name == 'SPNUMBER':
                self[name] = value
                # setattr(self, item, {name: value})  

            
def extract_edithist(dbf_path, id_field, output_type, start_date, end_date):
    # Regex to match edithist entries
    on_by_swap_pattern = '(\sby\s[A-Z]+__)(\son\s\d\d/\d\d/\d{4})'
    brahms_pattern = '([A-Z0-1\-\.]+):(.+?)(->|\sto\s)(.+?)\son\s(\d\d/\d\d/\d{4})\sby\s([A-Z]+)__'

    # Compile with ignorecase flag
    brahms_pattern = re.compile(brahms_pattern, re.IGNORECASE)
    on_by_swap_pattern = re.compile(on_by_swap_pattern, re.IGNORECASE)

    start = time.time()
    # Get brahms users list
    with open('users.csv') as f:
        users = f.read().splitlines()

    # BRAHMS sometimes uses 'short' usernames (truncated to 5 chars), sometimes long ones
    # Create a list of short (used in older brahms) usernames regex, we want to ignore longer ones. E.g. create Brend(?!a)
    users_short = [re.sub(r'(\w{5})(\w)\w+', r'\1(?!\2)', x) for x in users]
    # We have two users called Thuli - Thulis and ThuliM, remove them
    users_short.remove('Thuli(?!M)')
    users_short.remove('Thuli(?!s)')
    users_short_regex = '(' + '|'.join(users_short) + ')'
    user_regex = '(' + '|'.join(users) + ')'
    
    print('-------------')
    # Unfortunately can't ignore missing memo files as fucking edithist is one! , ignore_missing_memofile=True
    dbf = DBF(dbf_path, encoding='iso-8859-1', recfactory=Record) # parserclass=MyFieldParser,
    print('-------------')
    

    with open('output/' + output_type + '.csv', 'w', newline='') as csvfile:
        wr = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        wr.writerow(['date', 'user', 'brahms id', 'field'])
        
        for record in dbf:
            if record['EDITHIST'] is not None and record['EDITHIST'] != '':
                try:
                    ed = record['EDITHIST']
                    ed = re.sub(r'[\n\r]', '', ed)
                    ed = re.sub(users_short_regex, r'\1__', ed)
                    ed = re.sub(user_regex, r'\1__', ed)
                    
                    ed = ed.replace('Map point edit', 'MapPointEdit')
            
                    # For some reason the pattern is sometimes by x on date and sometimes on date by x... let's standardise
                    ed = re.sub(on_by_swap_pattern, r'\2\1', ed)
            
                    # Regex matches
                    for match in re.finditer(brahms_pattern, ed):
                        date = datetime.datetime.strptime(match.group(5).strip(), "%d/%m/%Y").date()
                        if date >= start_date and date <= end_date:
                            field = match.group(1).strip()			
                            user = match.group(6).strip()
                            wr.writerow([date, user, record[id_field], field])
                
                except UnicodeDecodeError:
                    import pdb; pdb.set_trace
                
    print('------' + output_type + ' COMPLETE -------')
    print(time.time() - start)
    print('-------------')

    
#extract_edithist('DATABASE\\species.dbf', 'SPNUMBER', 'species', start_date, end_date)
#extract_edithist('DATABASE\\people.dbf', 'ID', 'people', start_date, end_date)
extract_edithist('DATABASE\\collections.dbf', 'BRAHMS', 'botrecs', start_date, end_date)
