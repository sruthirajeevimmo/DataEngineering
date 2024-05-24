# Building a Comprehensive Sales Data Pipeline

## Description
This project aims to build a comprehensive sales data pipeline for data processing. It includes steps for installing necessary dependencies through running requirement.txt, creating databases, roles, and privileges, and executing scripts for data transformation. The project uses technologies such as PostgreSQL, Python, Pandas, SQLAlchemy, and Matplotlib to handle data processing tasks effectively. By following the provided instructions, you can set up and run the sales data pipeline. Also the project will create few visualisations that can be used to data analysis

## Installation Guide
1. Install PostgreSQL by running the following command:
        brew install postgresql@14
2. Start PostgreSQL version 14 by executing:
        brew services start postgresql@14
3. Access the PostgreSQL command line interface by running:
        psql postgres
4. Create the required database, user, and password as needed. Follow the steps below to create them. After step 3
    Run the below commands as in the same order
    # Create Role
    CREATE ROLE data_engineer WITH LOGIN PASSWORD 'data_1234';
    # Create DB Permission
    ALTER ROLE data_engineer CREATEDB;
    # List Users to validate
    \du
    # Exit using ctrl d
    # Login with data_engineer
    psql postgres -U data_engineer
    # Create database
    CREATE DATABASE sale_data_pipeline
   
5. Edit database.ini with the required details based on step 4
   host=localhost
   database=<< >>>>
   user=<< >>
   password=<<>>
   port=5432

6. Install Python dependencies by running:
        pip install -r requirements.txt


7. Execute the scripts star_schema_creation.py and main.py for further setup and data processing tasks.


## Design Guide
The complete Design Guide is [Link to Document](https://docs.google.com/document/d/162lUV1GhjqNqfAshrTTqNNqmOZCglqc6XY1EeqssSo0/edit)

## Contact Details
Please reach out to sruthirajeev2009@gmail.com incase of any issues.