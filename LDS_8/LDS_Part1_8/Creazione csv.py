import csv
import pycountry_convert as pc #libreria per trovare i nomi dei continenti
#pip install pycountry_convert

class geo_csv:
    
    def __init__(self, filepath):
        self.geo_output = open(filepath, 'w', encoding='utf-8-sig', newline='') 
        self.geo_writer = csv.writer(self.geo_output)
        self.geoid = 1
        self.geography = {} #we use a dictionary to avoid duplicate and retrive the id
        
    def header(self, write_header):
        self.geo_writer.writerow(write_header)

    def load(self, country_c, region):
        if country_c == 'uk': #the library not work with alpha_2 'uk', need to be changed to 'gb'
            country_code = 'gb'.upper()
        else:
            country_code = country_c.upper()  
        
        country_name = pc.country_alpha2_to_country_name(country_code)
        continent_code = pc.country_alpha2_to_continent_code(country_code)
        continent_name = pc.convert_continent_code_to_continent_name(continent_code) #find the continent name
        
        self.geo = (region, country_name, continent_name) #this tuple is used as key in the dictionary
        
        if self.geo not in self.geography:
            self.geography[self.geo] = self.geoid #add in the dictionary 
            self.geo_writer.writerow([self.geoid, region, country_name, continent_name])#write the row in the csv file
            self.geoid += 1
        
    def retrive_id(self):
        return self.geography[self.geo] #return the geoid for the corresponding tuple
    
    def terminate(self):
        self.geo_output.close()
        
    
class date_csv:
    
    def __init__(self, filepath):
        self.date_output = open(filepath, 'w', encoding='utf-8-sig', newline='') 
        self.date_writer = csv.writer(self.date_output)
        self.date = {}
        
    def header(self, write_header):
        self.date_writer.writerow(write_header)
    
    def find_quarter(self, month): #funcition to find the quarter
        if month >= 1 and month <= 3:
            return 'Q1'
        if month >= 4 and month <= 6:
            return 'Q2'
        if month >= 7 and month <= 9:
            return 'Q3'
        if month >= 10 and month <= 12:
            return 'Q4'
    
    def load_birth(self, dateofbirth):
        birth_split = dateofbirth.split(sep = '-') #[year, month, day] of the birth date
        birth_quarter = self.find_quarter(int(birth_split[1])) #find the quarter        
        
        self.birth_date = (dateofbirth, birth_split[2], birth_split[1], birth_split[0], birth_quarter) #[day, month, year] format for the database
        if self.birth_date not in self.date:
            date_id = birth_split[2] + birth_split[1] + birth_split[0] #DDMMYYYY id for the date
            self.date[self.birth_date] = date_id
            self.date_writer.writerow([date_id, dateofbirth, birth_split[2], birth_split[1], birth_split[0], birth_quarter])
            
    def load_answer(self, dateofanswer):
        simplified = dateofanswer.split(sep = ' ')[0] #remove the hour info '29-10-2022 17.37.00' --> '29-10-2022'
        answer_split = simplified.split(sep = '-') #[year, month, day] of the answer date
        answer_quarter = self.find_quarter(int(answer_split[1]))

        self.answer_date = (simplified, answer_split[2], answer_split[1], answer_split[0], answer_quarter) #[day, month, year] format for the database
        if self.answer_date not in self.date:
            date_id = answer_split[2] + answer_split[1] + answer_split[0] #DDMMYYYY id for the date
            self.date[self.answer_date] = date_id
            self.date_writer.writerow([date_id, simplified, answer_split[2], answer_split[1], answer_split[0], answer_quarter])
    
    def retrive_id(self, choose):
        if choose == 'birth':
            return self.date[self.birth_date] #return id of the birth date
        else:
            return self.date[self.answer_date] #return id of the answer date
        
    def terminate(self):
        self.date_output.close()


class subject_csv:

    def __init__(self, pathfile):
        self.subject_output = open(pathfile, 'w', encoding='utf-8-sig', newline='')
        self.subject_writer = csv.writer(self.subject_output) 
        self.subject = {}
        self.sub_id = 10001 #start from 10001 to avoid problems with the single SubjectId 
    
    def header(self, write_header):
        self.subject_writer.writerow(write_header)
    
    def recover_metadata(self):
        sub_meta_file = open('subject_metadata.csv', mode='r', encoding='utf-8-sig') #open the subject_metadata.csv file to read the data
        csv_subject = csv.DictReader(sub_meta_file, delimiter = ",")

        self.subject_dic = {} #store the information about the all subject to create the description

        for line in csv_subject:
            self.subject_dic[line['SubjectId']] = [line['Name'], line['ParentId'], line['Level']]

        sub_meta_file.close() #close the subject_metadata.csv file
    
    def generate_description(self, string): #function to generate the description of the subject question
        string = string.strip('][').split(', ') #convert the string rappresentation of the list in an actual list '[3, 5, 23, 106]' --> ['3', '5', '23', '106']
        subjects_list = [] 
        
        for e in string:
            subjects_list.append(self.subject_dic[e]) #retrive the subject from the code and store it in a list
            
        s = sorted(subjects_list, key = lambda x: x[2]) #order the subject based on the level in increasing order
        output = f'The question is about {s[3][0]}, part of the topic of {s[2][0]}, that is part of the subject {s[1][0]}, in the area of {s[0][0]}'
        return output
    
    def load(self, sub_string):
        description = self.generate_description(sub_string) #generate the description for the question
        if sub_string not in self.subject:
            self.subject[sub_string] = self.sub_id
            self.subject_writer.writerow([self.sub_id, description])
            self.sub_id += 1

    def retrive_id(self, sub_string):
        return self.subject[sub_string]
    
    def terminate(self):
        self.subject_output.close()


class organization_csv:
    
    def __init__(self, filepath):
        self.org_output = open(filepath, 'w', encoding='utf-8-sig', newline='')
        self.org_writer = csv.writer(self.org_output) 
        self.organization = {}
        self.org_id = 1
    
    def header(self, write_header):
        self.org_writer.writerow(write_header)
    
    def load(self, group, quiz, schemeofwork):
        self.org = (group, quiz, schemeofwork)
        if self.org not in self.organization:
            self.organization[self.org] = self.org_id
            self.org_writer.writerow([self.org_id, group, quiz, schemeofwork])
            self.org_id += 1
    
    def retrive_id(self):
        return self.organization[self.org]
    
    def terminate(self):
        self.org_output.close()

class user_csv:
    
    def __init__(self, filepath):
        self.user_output = open(filepath, 'w', encoding='utf-8-sig', newline='')
        self.user_writer = csv.writer(self.user_output) 
        self.users = set() #no need for a dictionary because we only need to not store duplicate, not to retrive the id
        
    def header(self, write_header):
        self.user_writer.writerow(write_header)
        
    def load(self, user_id, gender):
        if user_id not in self.users:
            self.users.add(user_id)
            self.user_writer.writerow([user_id, date_file.retrive_id('birth'), geo_file.retrive_id(), gender]) #date_file.retrive_id('birth') function of the date class to retrive the id of the birth date
    
    def terminate(self):
        self.user_output.close()
        
class answer_csv:
    
    def __init__(self, filepath):
        self.answer_output = open(filepath, 'w', encoding='utf-8-sig', newline='')
        self.ans_writer = csv.writer(self.answer_output)
        
    def header(self, write_header):
        self.ans_writer.writerow(write_header)
        
    def is_correct(self, given_value, correct_value):
        if given_value == correct_value: #create the variable correct and if the answer given == the correct one it is correct, otherwise not
            return 1 #the answer is correct
        else:
            return 0 #the answer is wrong
        
    def load(self, ans_id, questionid, userid, sub_string, answer_value, answer_correct, confidence):
        correct = self.is_correct(answer_value, answer_correct)
        ans = [ans_id, questionid, userid, org_file.retrive_id(), date_file.retrive_id('answer'), subject_file.retrive_id(sub_string), answer_value, answer_correct, correct, confidence]
        self.ans_writer.writerow(ans)
        
    def terminate(self):
        self.answer_output.close()

#Creazione file e scrittura header
geo_file = geo_csv('File csv/geography.csv')
geo_file.header(['geoid', 'region', 'country_name', 'continent'])

date_file = date_csv('File csv/date.csv')
date_file.header(['dateid', 'date', 'day', 'month', 'year', 'quarter'])

subject_file = subject_csv('File csv/subject.csv')
subject_file.header(['subjectid', 'description'])
subject_file.recover_metadata() #recover the subject metadata and store in a dictionary

org_file = organization_csv('File csv/organization.csv')
org_file.header(['organizationid', 'groupid', 'quizid', 'schemeofworkid'])

user_file = user_csv('File csv/user.csv')
user_file.header(['userid', 'dateofbirthid', 'geoid', 'gender'])

answer_file = answer_csv('File csv/answers.csv')
answer_file.header(['answerid', 'questionid', 'userid', 'organizationid', 'dateid', 'subjectid', 'answer_value', 'correct_answer', 'iscorrect', 'confidence'])

ans_file = open('answers_full.csv', mode='r', encoding='utf-8-sig') 
csv_answers_full = csv.DictReader(ans_file, delimiter = ",")

for line in csv_answers_full:
    
    geo_file.load(line['CountryCode'], line['Region'])
    
    date_file.load_birth(line['DateOfBirth'])
    date_file.load_answer(line['DateAnswered'])

    subject_file.load(line['SubjectId'])
    
    org_file.load(line['GroupId'], line['QuizId'], line['SchemeOfWorkId'])
    
    user_file.load(line['UserId'], line['Gender'])
    
    answer_file.load(line['AnswerId'], line['QuestionId'], line['UserId'], line['SubjectId'], line['AnswerValue'], line['CorrectAnswer'], line['Confidence'])
    
#chiusura file    
ans_file.close()
geo_file.terminate()
date_file.terminate()
subject_file.terminate()
org_file.terminate()
user_file.terminate()
answer_file.terminate()

print("Done")