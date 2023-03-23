import pandas as pd
import boto3  # Importing boto3 library for interacting with AWS services
import os
from dotenv import load_dotenv


# Loading environment variables from .env file into the script
load_dotenv()

# Creating an S3 resource object to interact with the S3 bucket
s3 = boto3.resource(
    service_name="s3",
    region_name="us-east-2",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)


# function to extract data from S3 Bucket
def extractData():
    # Define the filename for the downloaded Excel file
    filename = "extracted_data/employee_data.xlsx"

    # Download the Excel file from S3 to the local machine
    s3.Bucket("unthinkable-mayank-test").download_file(
        Key="de_test1234/employee_1.xlsx", Filename=filename
    )

    # Read the downloaded Excel file into a Pandas DataFrame
    df = pd.read_excel(filename)

    # Save the DataFrame to a CSV file
    df.to_csv("test/all_emp/all_data.csv", index=False)

    # Print the DataFrame to the console
    print(df.to_string())

    # Return a dictionary with a message indicating that the function has completed
    return {"message": "done"}


# function to transform data as per task
def transformData():
    df = pd.read_csv("test/all_emp/all_data.csv")

    # finding unique ids of all employees in the data
    unique_ids = df["employee id"].unique()

    # creating a dictionary to store data of each employee
    employee_data = {}

    # looping through unique employee ids to get data of each employee
    for employee_id in unique_ids:
        # getting data of a specific employee using employee id
        employee_df = df[df["employee id"] == employee_id]

        # storing the data of the employee in the dictionary
        employee_data[employee_id] = employee_df

    # looping through the data of each employee to calculate average, in time and out time1
    for employee_id, employee_df in employee_data.items():
        # calcuating average, in_time and out_time from data
        average = employee_df["area"] * employee_df["total income"]
        in_time = employee_df["area"] * employee_df["in time(days)"]
        out_time1 = employee_df["area"] * employee_df["out time"]

        # adding new columns into the employee_df dataframe
        employee_df["average"] = average
        employee_df["in time"] = in_time
        employee_df["out time1"] = out_time1

    # creating a dictionary to store transformed data of each employee
    transformed_data = {}

    # looping through the data of each employee to store transformed data in the dictionary
    for employee_id, employee_df in employee_data.items():
        # storing the transformed data of the employee in the dictionary
        transformed_data[employee_id] = employee_df

    # creating a list to store results of each employee
    employee_results = []

    # looping through the transformed data of each employee to calculate the results and store them in the list
    for employee_id, employee_df in transformed_data.items():
        # calculating sum of out time1, in_time, average and area
        sum_out_time = employee_df["out time1"].sum()
        sum_in_time = employee_df["in time"].sum()
        sum_average = employee_df["average"].sum()
        sum_area = employee_df["area"].sum()

        # calculating transformed out time, in_time and average
        out_time_transformed = sum_out_time / sum_area
        in_time_transformed = sum_in_time / sum_area
        average_transformed = sum_average / sum_area

        # getting the minimum value of average
        min_average = employee_df["average"].min()

        # storing the results of the employee in the list
        employee_results.append(
            {
                "employee_id": employee_id,
                "average": average_transformed,
                "lowest_average": min_average,
                "out time": out_time_transformed,
                "in time": in_time_transformed,
            }
        )

    # creating a dataframe from the list of results
    results_df = pd.DataFrame(employee_results)
    print(results_df)

    # saving the results to a csv file
    results_df.to_csv("test/transformed/employee_transformed_data.csv", index=False)
    return {"message": "data_transformed"}


# function to laod data into S3 bucket
def loadData():
    # Read the transformed data CSV file
    df = pd.read_csv("test/transformed/employee_transformed_data.csv")

    # Sort the DataFrame by employee_id column in ascending order
    sorted_df = df.sort_values(by=["employee_id"], ascending=True)

    # Save the sorted DataFrame to an Excel file
    sorted_df.to_excel(
        "test/final_data/employee_transformed_excel_data.xlsx", index=False
    )

    # Upload the Excel file to S3 bucket
    # s3.Bucket('unthinkable-mayank-test').upload_file(
    #     Key="output/kartik-lavkush.xlsx", Filename="test/final_data/employee_transformed_excel_data.xlsx"
    # )
    print(sorted_df)
    return {"message": "data_loaded"}