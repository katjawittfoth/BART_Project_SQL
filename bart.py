import psycopg2
import glob
import xlrd
import zipfile
import os
import shutil
import re
import calendar

def ProcessBart(tmpDir, dataDir, SQLConn, schema, table):
    existingfiles = glob.glob('path/.*')
    for f in existingfiles:
        os.remove(f)

    # finding all zip files under dataDir
    zipFilePath = findFilePath(dataDir)
    zipFilePath = list(filter(lambda x: x.endswith('.zip'), zipFilePath))

    # unzip the zip files
    for file in zipFilePath:
        with zipfile.ZipFile(file, "r") as zip_ref:
            zip_ref.extractall(tmpDir)

    # finding all excel files
    excelFilePath = findFilePath(tmpDir)
    excelFilePath = list(filter(lambda x: x.endswith('.xls') or x.endswith('.xlsx'), excelFilePath))

    monthintdict = {v: k for k, v in enumerate(calendar.month_name) if k is not 0}

    csv_string = ''

    for filepath in excelFilePath:
        my_book = xlrd.open_workbook(filepath)

        # get month-year information from the file name
        filename = os.path.basename(filepath)
        match = re.search('[A-Za-z]+ ?\d+', filename)
        mon_year_combo = []
        if match:
            mon_year = match.group(0)
            if " " in mon_year:
                mon_year_combo = mon_year.split()
            else:
                mon_year_combo.append(mon_year[:-4])
                mon_year_combo.append(mon_year[-4:])

        for sheet in my_book.sheet_names():
            # read in daytype info
            if 'Fast Pass' in sheet or 'FP' in sheet:
                break
            if ("Wkdy" in sheet or "Weekday" in sheet):
                daytype = "Weekday"
            elif "Sat" in sheet:
                daytype = "Saturday"
            else:
                daytype = "Sunday"

            # count the number of stations
            nstations = 0
            for row in my_book.sheet_by_name(sheet).col_values(0):
                if row == 'Entries':
                    break
                nstations += 1

            for j in range(1, nstations - 1):
                for i in range(2, nstations):
                    row_name = my_book.sheet_by_name(sheet).col_values(0)[i]
                    col_name = my_book.sheet_by_name(sheet).row_values(1)[j]
                    if type(row_name) is float:
                        row_name = str(int(row_name))
                    else:
                        row_name = str(row_name)
                    if type(col_name) is float:
                        col_name = str(int(col_name))
                    else:
                        col_name = str(col_name)

                    try:
                        csv_string += (str(monthintdict[mon_year_combo[0]]) + ',' + str(mon_year_combo[1]) + ',' + daytype + ','
                            + col_name + ','
                            + row_name + ','
                            + str(round((my_book.sheet_by_name(sheet).cell_value(i, j)))+ "\n"))
                    except:
                        csv_string += (str(monthintdict[mon_year_combo[0]]) + ',' + str(
                            mon_year_combo[1]) + ',' + daytype + ','
                                       + col_name + ','
                                       + row_name + ','
                                        + "\n")


    # write the lists to csv file
    f = open(tmpDir + 'toLoad.csv', 'w')
    f.write(csv_string)  # Give your csv text here.
    # Python will convert \n to os.linesep
    f.close()

    # create table using psycopg2, load into DB
    SQLCursor = SQLConn.cursor()
    SQLCursor.execute("""
       CREATE TABLE %s.%s
       (
       mon int
       , yr int
       , daytype varchar(15)
       , start varchar(2)
       , term varchar(2)
       , riders float
       );""" % (schema, table))
    SQLCursor.execute("""COPY %s.%s FROM '%s' CSV;"""
                      % (schema, table, tmpDir + 'toLoad.csv'))
    SQLConn.commit()



def findFilePath(dir):
    filenamelist = []
    for (root, dirs, filenames) in os.walk(dir):
        for file in filenames:
            filenamelist.append(os.path.join(root, file))
    return filenamelist

ProcessBart('/Users/Katja/USF/Module1/SQL/BART/test/',
            '/Users/Katja/USF/Module1/SQL/BART/BART DATA/', SQLConn=LCLconnR, schema='cls', table='bart')
