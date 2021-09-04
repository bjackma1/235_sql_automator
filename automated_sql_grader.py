# -*- coding: utf-8 -*-
# !pip install mysql
# !pip install sqlalchemy

import pymysql
import re
import os
import pandas as pd
from sqlalchemy import create_engine, exc
import time

SQL_1 = ['SELECT * FROM Customers;',
         'SELECT DISTINCT ContactTitle FROM Suppliers;',
         '''SELECT CompanyName, 
              ContactName, 
              City 
            FROM Customers WHERE City = "London";''',
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
            WHERE ContactTitle LIKE "%%sales%%" 
            ORDER BY CompanyName;''',
         '''SELECT CompanyName, 
              ContactName, 
              City, 
              Country, 
              Fax 
            FROM Customers 
            WHERE Fax IS NOT null;''',
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
            AND City NOT IN ('Campinas', 'Portland', 'Vancouver');''']

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
                    WHERE ContactTitle LIKE "%%Manager%%" AND ContactTitle NOT LIKE "%%Sales%%")
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
        with open(filepath, 'r') as file_handler:
            str_file = file_handler.read()
            # adding a /* to make the regex easier so it just looks for everything between the end of the comments,
            # which is everything between */ and /*
            str_file += '/*'
            str_file = str_file.strip().replace('%', '%%').replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
            return re.findall('(?s)(?<=\*/).*?(?=/\*)', str_file)
    else:
        pass


def run_query(query: str) -> pd.DataFrame:
    """runs the inputted query and converts all of the columns in the result to strings so that it can be compared
       correctly to the correct dataframe"""
    try:
        query_result = pd.read_sql_query(query, db_connection)
    except exc.ProgrammingError as syntax_error:
        print(f'SQL SYNTAX ERROR IN THIS QUERY -- {syntax_error}')
        return pd.DataFrame()
    except exc.OperationalError as operational_error:
        print(f'SQL OPERATIONAL ERROR IN THIS QUERY -- {operational_error}')
        return pd.DataFrame()

    for column in query_result.columns:
        try:
            query_result[column] = query_result[column].astype(str)
        except Exception as excep:
            print(excep)
            return pd.DataFrame
    return query_result


def evaluate_student_queries(hw_number: int, submission: str) -> None:
    """gets the queries from the student's submission and compares them to the correct queries stored locally"""

    student_name = re.search('^(.+?)_', submission.split('/')[-1]).group(0)[:-1]
    print(student_name.capitalize())
    if hw_number == 1:
        answer_filepath = './sql_answers/sql_1_answers/'
    elif hw_number == 2:
        answer_filepath = './sql_answers/sql_2_answers/'
    else:
        print('invalid homework number entered')
        exit()

    # we need to remove the first column of the csv in order to make sure the two responses will be identical
    # because converting a dataframe to a csv leaves us with a useless index column in row 1
    correct_answers = [pd.read_csv(answer_filepath+path).iloc[:, 1:] for path in os.listdir(answer_filepath)]

    student_queries = []
    student_query_responses = []
    for query in get_individual_queries(submission):
        student_queries.append(query)
        if "#" in query:
            query = query[query.index("#")+1:]
        try:
            student_query_responses.append(run_query(query))
        except UnicodeDecodeError as unicode_decode_error:
            student_query_responses.append([])
            print(unicode_decode_error)
        except exc.ResourceClosedError as resource_closed_error:
            student_query_responses.append([])
            print(resource_closed_error)
            print('i actually have no clue what causes this and i do not care enough to look into it. just grade it '
                  'manually')
            print(f'query is {query}')

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
                    print(f'CORRECT -- {student_name} QUESTION {i + 1} IS CORRECT\n')
                else:
                    # a lot of students didn't put Fax in SQL 1 but I am not gonna mark them off for that
                    correct_answers[i].columns = [col.casefold() for col in correct_answers[i].columns]
                    student_query_responses[i].columns = [col.casefold() for col in student_query_responses[i].columns]
                    diff_cols = list(set(correct_answers[i].columns) - set(student_query_responses[i].columns))
                    if diff_cols == ['fax']:
                        print(f'CORRECT -- {student_name} QUESTION {i + 1} IS CORRECT')
                        continue
                    # not gonna penalize students for selecting more columns than necessary
                    same_columns = set(correct_answers[i].columns).issubset(set(student_query_responses[i].columns))
                    same_rows = len(correct_answers[i]) == len(student_query_responses[i])
                    if same_rows and same_columns:
                        print(f'CORRECT -- {student_name} QUESTION {i + 1} IS CORRECT\n')
                        continue
                    print(f'{student_name} question {i + 1} is incorrect')
                    if same_rows:
                        print('responses have same number of rows')
                    else:
                        print('responses have different number of rows')
                    if same_columns:
                        print('responses have same number of columns')
                    else:
                        print(correct_answers[i].columns)
                        print(student_query_responses[i].columns)
                        print(f'COLUMNS {diff_cols if len(diff_cols) >=1 else None} ARE NOT IN STUDENT SUBMISSION')
                        print('responses have different number of columns')
                    print()
            except IndexError as index_error:
                print(f'student query responses and correct answers do not have a matching index --- {index_error}')


def export_correct_answers_to_csv() -> None:
    """YOU SHOULD NOT HAVE TO RUN THIS. YOU SHOULD ALREADY HAVE ACCESS TO THE CSVs WITH THE CORRECT ANSWERS.

       instead of querying the db for the correct answer every time, I am just going to keep csvs of all the
       correct answers so the program doesn't take 10 billion years to run"""
    for question_number, query in enumerate(SQL_1):
        run_query(query).to_csv(f'./sql_answers/sql_1_answers/{int(question_number) + 1}.csv')
    for question_number, query in enumerate(SQL_2):
        run_query(query).to_csv(f'./sql_answers/sql_2_answers/{int(question_number) + 1}.csv')


def main() -> None:
    start_time = time.time()
    homework_number = int(input("what homework number is this? "))
    student_directory = input("what is the filepath that contains all of the .sql files? ")
    # student_directory = '/Users/markjackman/Downloads/submissions 2/'
    # homework_number = 1
    if student_directory[-1] != '/':
        student_directory += '/'
    convert_sql_to_txt(student_directory)
    # evaluate_student_queries(homework_number, 'test_sql.txt')
    for submission in os.listdir(student_directory):
        evaluate_student_queries(homework_number, student_directory+submission)
    print(f'this code took {time.time() - start_time} seconds to process')


if __name__ == '__main__':
    main()
