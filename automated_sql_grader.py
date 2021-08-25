# -*- coding: utf-8 -*-
# !pip install mysql
# !pip install sqlalchemy
# !pip install pandas

import pymysql
import re
import os
import pandas as pd
from sqlalchemy import create_engine, exc
import time

# these lists are just the lists of the correct queries from Sopha that are used to generate the csvs of the correct responses
SQL_1 = ['SELECT * FROM Customers;',
         'SELECT DISTINCT ContactTitle FROM Suppliers;',
         '''SELECT CompanyName, 
              ContactName, 
              City 
            FROM Customers WHERE City = "London";'''
         '''SELECT OrderID, 
              OrderDate, 
              ShippedDate, 
              CustomerID, 
              Freight 
            FROM Orders 
            ORDER BY Freight DESC;''',
         '''SELECT CompanyName, 
              ContactName, 
              City 
            FROM Customers 
            WHERE City = "London";''',
         '''SELECT City, 
              CompanyName, 
              ContactName, 
              ContactTitle 
            FROM Customers 
            WHERE ContactTitle LIKE "%sales%" 
            ORDER BY CompanyName;''',
         '''SELECT CompanyName, 
              ContactName, 
              City, 
              Country, 
              Fax 
            FROM Customers 
            WHERE Fax IS NOT NULL;''',
         '''SELECT CompanyName, 
              ContactName, 
              City, 
              Country, 
              Fax 
            FROM Customers 
            WHERE Country = "Germany" AND Fax IS NOT NULL;''',
         '''SELECT CompanyName, 
              ContactName, 
              City, 
              Country, 
              Region 
            FROM Customers
            WHERE Country IN ('USA', 'Canada', 'Mexico', 'Argentina', 'Brazil', 'Venezuela') 
            AND City NOT IN ('Campinas', 'Portland', 'Vancouver')''']

SQL_2 = ['''SELECT e.LastName, 
              o.OrderID, 
              o.ShipName, 
              o.ShipCountry, 
              o.Freight 
            FROM Employees e 
            INNER JOIN Orders o 
              ON e.EmployeeID = o.EmployeeID
            INNER JOIN Shippers s 
              ON s.ShipperID = o.ShipVia 
            WHERE ShipCountry IN ('UK', 'Ireland');''',
         'SELECT DISTINCT Country, COUNT(ContactName) FROM Customers GROUP BY Country;',
         'SELECT * From Products WHERE (UnitPrice > 20) AND (UnitPrice < 50) AND (UnitsInStock > 0);',
         '''SELECT Count(CustomerID), ContactTitle 
            FROM Customers 
            WHERE ContactTitle LIKE "%%Manager%%" 
            GROUP BY ContactTitle;''',
         '''SELECT Count(CustomerID), ContactTitle FROM Customers WHERE CustomerID IN 
                (SELECT CustomerID FROM Customers 
                    WHERE ContactTitle LIKE "%%Manager%%" AND ContactTitle NOT LIKE "%%Sales%%") AS a
                GROUP BY ContactTitle;''',
         '''SELECT sum((d.UnitPrice * (d.UnitPrice * d.Discount) * d.Quantity)), 
              d.OrderID, 
              o.CustomerID, 
              o.ShipName 
            FROM OrderDetails d
            INNER JOIN Orders o 
              ON d.OrderID = o.OrderID  
            GROUP BY OrderID  
            ORDER BY CustomerID;''',
         '''SELECT d.OrderID AS 'Order Number',
              o.CustomerID AS 'Customer Number',
              o.ShipName AS 'Customer Recipient',
              SUM((d.UnitPrice - (d.UnitPrice * d.Discount)) * d.Quantity) AS 'Order Total',
              e.TitleOfCourtesy AS 'Employee Personal Title',
              e.FirstName AS 'Employee First Name',
              e.LastName AS 'Employee Last Name'
            FROM OrderDetails AS d
            INNER JOIN Orders AS o
              ON d.OrderID = o.OrderID
            INNER JOIN Employees AS e
              ON e.EmployeeID = o.EmployeeID
            GROUP BY d.OrderId
            ORDER BY e.LastName;''']

# creating db connection
try:
    db_connection = create_engine('mysql+pymysql://cis23xstudent:Da7aB8isGr8!@sql.wpc-is.online:3306/Northwind',
                                  echo=False)
except Exception as e:
    print(e)
    print("Could Not Create Database Connection.")
    exit()


def convert_sql_to_txt(directory: str) -> None:
    """converts all sql files in submission folder to plain text files so we can append /* to make regex easier"""
    for filepath in os.listdir(directory):
        base = os.path.splitext(filepath)[0]
        os.rename(directory + filepath, directory + base + '.txt')


def get_individual_queries(filepath: str) -> list:
    """gets rid of the comments in the file and returns a list of SQL queries to be run"""
    if filepath != ".DS_Store.txt":
        print(filepath)
        with open(filepath, 'a') as appending_file:
            # we are just writing a /* to the end of the file so our regex can be simpler
            appending_file.write('/*')

        with open(filepath, 'r') as test_file:
            # regex looks for everything between the end of the question description and beginning of a new one (*/, */)
            # those queries are then cleaned and then returned in list format
            return [query.strip() for query in re.findall('(?s)(?<=\*/).*?(?=/\*)', test_file.read())]
    else:
        return


def run_query(query: str, author: str = '') -> list:
    """runs the inputted query and converts all of the columns in the result to strings so that it can be compared
       correctly to the correct dataframe"""
    try:
        query_result = pd.read_sql_query(query, db_connection)
    except exc.ProgrammingError as syntax_error:
        print(f'SQL SYNTAX ERROR IN THIS QUERY -- {syntax_error}')
        return []

    for column in query_result.columns:
        try:
            query_result[column] = query_result[column].astype(str)
        except Exception as excep:
            print(excep)
            return []
    return query_result


def evaluate_student_queries(hw_number: int, student_file: str) -> None:
    """gets the queries from the student's submission and compares them to the correct queries stored locally"""

    # when you download submissions, the student's name is always first, followed by a _, so this checks for that
    student_name = re.search('^(.+?)_', student_file).group(0)[:-1]

    # this may have to change based on how your directory
    if hw_number == 1:
        answer_filepath = './sql_answers/sql_1_answers/'
    else:
        answer_filepath = './sql_answers/sql_2_answers/'


    # we need to remove the first column of the csv in order to make sure the two responses will be identical
    correct_answers = [pd.read_csv(answer_filepath+path).iloc[:, 1:] for path in os.listdir(answer_filepath)]

    try:
        student_query_responses = [run_query(query) for query in get_individual_queries(student_file)]
    except UnicodeDecodeError as unicode_decode_error:
        student_query_responses = []
        print(unicode_decode_error)

    # dataframes technically aren't equal if their columns are of different types, so we are just converting
    # everything to strings
    for correct_answer, student_answer in zip(correct_answers, student_query_responses):
        for correct_col, student_col in zip(correct_answer, student_answer):
            correct_answer[correct_col] = correct_answer[correct_col].astype(str)
            student_answer[student_col] = student_answer[student_col].astype(str)

    for i in range(len(student_query_responses)):
        if not isinstance(student_query_responses[i], list):
            try:
                if correct_answers[i].equals(student_query_responses[i]):
                    print(f'{student_name} question {i + 1} is correct')
                else:
                    # print(correct_answers[i].info())
                    # print(student_query_responses[i].info())
                    print(f'{student_name} question {i + 1:^10} is incorrect')
                    if len(correct_answers[i]) == len(student_query_responses[i]):
                        print('responses have same number of rows')
                    else:
                        print('responses have different number of rows')
                    if len(correct_answers[i].columns) == len(student_query_responses[i].columns):
                        print('responses have same number of columns')
                    else:
                        print('responses have different number of columns')
            except IndexError as index_error:
                print(f'student query responses and correct answers do not have a matching index --- {index_error}')


def export_correct_answers_to_csv(correct_answer_directory: str) -> None:
    """instead of querying the db for the correct answer every time, I am just going to keep csvs of all the
       correct answers so the program doesn't take 10 billion years to run
       
       i will just upload them at a later point"""
    for question_number, query in enumerate(SQL_1):
        run_query(query).to_csv(f'./sql_answers/sql_1_answers/{int(question_number) + 1}.csv')
    for question_number, query in enumerate(SQL_2):
        run_query(query).to_csv(f'./sql_answers/sql_2_answers/{int(question_number) + 1}.csv')
   
   
if __name__ == '__main__':
    start_time = time.time()
    homework_number = int(input("what homework number is this? "))
    # student_filepath = input("what is the filepath that contains all of the .sql files? ")
    student_directory = ''
    convert_sql_to_txt(student_directory)
    evaluate_student_queries(homework_number, 'test_sql.txt')
    for submission in os.listdir(student_directory):
        evaluate_student_queries(homework_number, student_directory + submission)
    print(f'this code took {time.time() - start_time} seconds to process')
