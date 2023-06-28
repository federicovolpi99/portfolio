import pyodbc
import csv

class upload:
     
    def __init__(self, server, database, username, password):
        connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
        self.conn = pyodbc.connect(connectionString)
        self.cursor = self.conn.cursor() 
        self.cursor.fast_executemany = True
         
    def check_empty(self, table): #if the table is not empty delete all rows, if it is already empty do nothing
        check = self.cursor.execute('SELECT TOP 1 * from [Group_8].[' + table + ']') 
        if check.fetchone() is not None: 
            self.cursor.execute('DELETE FROM [Group_8].[' + table + ']')
            self.conn.commit()

    def check_row(self, table, csv_row): #check if the rows in the table are equal to the rows in the csv file
        database_row = self.cursor.execute('SELECT COUNT(*) FROM [Group_8].[' + table + ']').fetchone()[0]
        if database_row == csv_row:
            print(table + ' table loaded correctly')
        else:
            print('Something went wrong with ' + table)
            
    def geography(self):
        file_geo = open('File csv/geography.csv', mode='r', encoding='utf-8-sig') 
        geodata = csv.DictReader(file_geo, delimiter = ',')
        csv_row = 0 #count the csv rows 
        
        self.check_empty('Geography') #check if the table is empty or not
        
        data_to_load = [] #inizialize a list to store the csv values
        for line in geodata: # Insert data into Table
            csv_row += 1    
            row_data = (line['geoid'], 
                        line['region'],
                        line['country_name'],
                        line['continent']) #inset the row of the csv in a tuple
            data_to_load.append(row_data) #append the tuple in the list
            
        query = '''INSERT INTO [Group_8].[Geography] 
                (geoid, region, country_name, continent)
                VALUES (?,?,?,?)'''
                
        self.cursor.executemany(query, data_to_load) #load all the data in the table
        self.conn.commit()
        
        self.check_row('Geography', csv_row) #check if the load was done correctly
        
        file_geo.close()

    def date(self):
        file_date= open('File csv/date.csv', mode='r', encoding='utf-8-sig') 
        datedata = csv.DictReader(file_date, delimiter = ',')
        csv_row = 0
        
        self.check_empty('Date')
        
        data_to_load = []
        for line in datedata: 
            csv_row += 1    
            row_data = (line['dateid'], 
                        line['date'], 
                        line['day'],
                        line['month'],
                        line['year'],
                        line['quarter'])
            data_to_load.append(row_data)


        query = '''INSERT INTO [Group_8].[Date] 
                (dateid, date, day, month, year, quarter)
                VALUES (?,?,?,?,?,?)'''
                
        self.cursor.executemany(query, data_to_load)
        self.conn.commit()
            
        self.check_row('Date', csv_row)
               
        file_date.close()
        
    def subject(self):
        file_subject= open('File csv/subject.csv', mode='r', encoding='utf-8-sig') 
        subdata = csv.DictReader(file_subject, delimiter = ',')
        csv_row = 0
        
        self.check_empty('Subject')
        
        data_to_load = []
        for line in subdata: # Insert data into Table
            csv_row += 1    
            row_data = (line['subjectid'], 
                        line['description'])
            data_to_load.append(row_data)
            
        query = '''INSERT INTO [Group_8].[Subject] 
                (subjectid, description)
                VALUES (?,?)'''
                       
        self.cursor.executemany(query, data_to_load)
        self.conn.commit()
            
        self.check_row('Subject', csv_row)
        
        file_subject.close()
    
    def organization(self):
        file_org= open('File csv/organization.csv', mode='r', encoding='utf-8-sig') 
        orgdata = csv.DictReader(file_org, delimiter = ',')
        csv_row = 0
        
        self.check_empty('Organization')
        
        data_to_load = []
        for line in orgdata: 
            csv_row += 1    
            row_data = (line['organizationid'], 
                        line['groupid'], 
                        line['quizid'],
                        line['schemeofworkid'])
            data_to_load.append(row_data)
            
        query = '''INSERT INTO [Group_8].[Organization] 
                (organizationid, groupid, quizid, schemeofworkid)
                VALUES (?,?,?,?)'''
        
        self.cursor.executemany(query, data_to_load)
        self.conn.commit()
            
        self.check_row('Organization', csv_row)
               
        file_org.close()
    
    def user(self):
        file_user= open('File csv/user.csv', mode='r', encoding='utf-8-sig') 
        userdata = csv.DictReader(file_user, delimiter = ',')
        csv_row = 0
        
        self.check_empty('User')
        
        data_to_load = []
        for line in userdata: 
            csv_row += 1    
            row_data = (line['userid'], 
                        line['dateofbirthid'], 
                        line['geoid'],
                        line['gender'])
            data_to_load.append(row_data)
            
        query = '''INSERT INTO [Group_8].[User] 
                (userid, dateofbirthid, geoid, gender)
                VALUES (?,?,?,?)'''
                
        self.cursor.executemany(query, data_to_load)
        self.conn.commit()
            
        self.check_row('User', csv_row)
               
        file_user.close()
        
    def answers(self):
        file_answers= open('File csv/answers.csv', mode='r', encoding='utf-8-sig') 
        ansdata = csv.DictReader(file_answers, delimiter = ',')
        csv_row = 0
        
        self.check_empty('Answers')
        
        data_to_load = []
        for line in ansdata: 
            csv_row += 1    
            row_data = (line['answerid'], 
                        line['questionid'], 
                        line['userid'],
                        line['organizationid'],
                        line['dateid'], 
                        line['subjectid'], 
                        line['answer_value'],
                        line['correct_answer'], 
                        line['iscorrect'],
                        line['confidence'])
            data_to_load.append(row_data)
            
        query = '''INSERT INTO [Group_8].[Answers] 
                (answerid, questionid, userid, organizationid, dateid, subjectid, answer_value, correct_answer, iscorrect, confidence)
                VALUES (?,?,?,?,?,?,?,?,?,?)'''
                 
        self.cursor.executemany(query, data_to_load)
        self.conn.commit()
            
        self.check_row('Answers', csv_row)
               
        file_answers.close()
        
    def close(self):
       self.cursor.close()
       self.conn.close()
       
connection = upload('lds.di.unipi.it', 'Group_8_DB', 'Group_8', 'AUB352QT') 

connection.geography()
connection.date()
connection.subject()
connection.organization()
connection.user()
connection.answers()

connection.close()