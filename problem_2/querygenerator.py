import pandas as pd
from sqlalchemy import Float,create_engine, Table, MetaData, insert, Column, String,or_, select, func, case, Date, Integer, cast, and_, exists,delete
from typing import List
from generate_sample import company_employee_info_df, events_df, companies_df
from sqlalchemy.orm import Session
import re
from tabulate import tabulate
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
# replace with your string to run the code
db_connection_string = "postgresql://postgres:your_password@localhost:5433/event_people"
engine = create_engine(db_connection_string)

metadata = MetaData()

event_attributes = Table('event_attributes', metadata,
    Column('event_url', String),
    Column('attribute', String),
    Column('value', String)
)

company_attributes = Table('company_attributes', metadata,
    Column('company_url', String),
    Column('attribute', String),
    Column('value', String)
)

person_attributes = Table('person_attributes', metadata,
    Column('person_id', String),
    Column('attribute', String),
    Column('value', String)
)

metadata.create_all(engine)

def insert_data(df, table):
    with Session(engine) as session:
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            stmt = insert(table).values(row_dict)
            session.execute(stmt)
        session.commit()
# function to build query for entity attribute value model
# creates a set of all columns by combining filter_args and output_cols
# The function iterates through the filter arguments to apply conditions
# checks if column belongs to the current table.
def build_query(table, columns, filter_arguments):
    url_column = {
        'event_attributes': 'event_url',
        'company_attributes': 'company_url',
        'person_attributes': 'person_id'
    }.get(table.name, None)
    
    if url_column is None:
        raise ValueError(f"Unknown table: {table.name}")
    filter_columns = {col for col, _, _ in filter_arguments}

    all_columns = set(columns) | filter_columns

    all_columns.add(url_column)
    all_columns=set(all_columns)
    all_columns=list(all_columns)
    pivot_subquery = select(
    table.c[url_column].label(f"{table.name}_id"),  # Give a unique label to the ID column
    *[func.max(case((table.c.attribute == col, table.c.value), else_=None)).label(col)
      for col in all_columns if col != url_column]  # Exclude the URL column from pivoting
).group_by(table.c[url_column]).alias('pivot')
    main_query = select(pivot_subquery)

    filter_conditions = []
    print(filter_arguments)
    for column, condition, value in filter_arguments:
        if column.startswith(f"{table.name.split('_')[0]}_"):
            print(table)
            if condition == 'includes':
                if isinstance(value, list):
                    if len(value) > 1:
                        print(value)
                        # Use OR for multiple values
                        filter_conditions.append(or_(*[pivot_subquery.c[column] == v for v in value]))
                    else:
                        filter_conditions.append(pivot_subquery.c[column] == value[0])
                else:
                    filter_conditions.append(pivot_subquery.c[column] == value)
            elif condition in ['greater-than-equal-to', 'less-than-equal-to']:
                op = '>=' if condition == 'greater-than-equal-to' else '<='
                if column.endswith('_date'):
                    filter_conditions.append(cast(pivot_subquery.c[column], Date) <= value if op == '<=' else cast(pivot_subquery.c[column], Date) >= value)
                else:
                    filter_conditions.append(cast(pivot_subquery.c[column], Integer) <= int(value) if op == '<=' else cast(pivot_subquery.c[column], Integer) >= int(value))

    if filter_conditions:
        main_query = main_query.where(and_(*filter_conditions))

    if url_column in columns:
        main_query = main_query.add_columns(pivot_subquery.c[f"{table.name}_id"].label(url_column))

    return main_query
# builds query separately depending upon whether the condition has to be applied on events/attributes/people
# executes the queries 
def query_data(filter_arguments: pd.DataFrame, output_columns: List[str]) -> dict:
    filter_arguments_list = filter_arguments.values.tolist()

    with engine.begin() as connection:
        for table in [event_attributes, company_attributes, person_attributes]:
            connection.execute(delete(table))
    
    insert_data(events_df, event_attributes)
    insert_data(companies_df, company_attributes)
    insert_data(company_employee_info_df, person_attributes)

    tables_needed = set()
    for column in output_columns + [arg[0] for arg in filter_arguments_list]:
        if column.startswith('event_'):
            tables_needed.add('event_attributes')
        elif column.startswith('company_'):
            tables_needed.add('company_attributes')
        elif column.startswith('person_'):
            tables_needed.add('people_attributes')

    queries = []
    results = {}

    if 'event_attributes' in tables_needed:
        event_filters = [f for f in filter_arguments_list if f[0].startswith('event_')]
        event_columns = [col for col in output_columns if col.startswith('event_')]
        query = build_query(event_attributes, event_columns, event_filters)
        queries.append(("Events", query))

    if 'company_attributes' in tables_needed:
        company_filters = [f for f in filter_arguments_list if f[0].startswith('company_')]
        company_columns = [col for col in output_columns if col.startswith('company_')]
        query = build_query(company_attributes, company_columns, company_filters)
        queries.append(("Companies", query))

    if 'people_attributes' in tables_needed:
        people_filters = [f for f in filter_arguments_list if f[0].startswith('person_')]
        people_columns = [col for col in output_columns if col.startswith('person_')]
        query = build_query(person_attributes, people_columns, people_filters)
        queries.append(("Person", query))

    results = {}

    def execute_query(query_name, query):
        with engine.connect() as connection:
            print(f"\n--- {query_name} Query ---")
            print(query)
            
            result = connection.execute(query)
            rows = result.fetchall()
            columns = result.keys()
            
            df = pd.DataFrame(rows, columns=columns)
            # Filter the DataFrame to include only the columns in output_columns
            df = df[[col for col in df.columns if col in output_columns]]
            return query_name, df

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(execute_query, query_name, query): query_name for query_name, query in queries}
        for future in as_completed(futures):
            query_name = futures[future]
            try:
                query_name, df = future.result()
                results[query_name] = df
            except Exception as e:
                print(f"Query failed: {e}")
    return results
# example 
filter_args_df = pd.DataFrame([
    ['event_continent', 'includes', ['Europe']],
    ['event_industry', 'includes', ['Finance']],
    ['event_start_date', 'greater-than-equal-to', '2025-07-01'],
    ['event_start_date', 'less-than-equal-to', '2025-12-31'],
    ['company_industry', 'includes', ['Healthcare', 'Biotech', 'Medtech']],
    ['person_seniority', 'includes', ['Junior']],
    ['company_revenue', 'less-than-equal-to', 2000],


], columns=['column', 'condition', 'value'])

output_cols = ['event_name','person_seniority', 'event_industry', 'event_city', 'event_continent', 'event_country', 'event_start_date', 'company_name', 'company_industry', 'company_revenue','company_url']

result_dict = query_data(filter_args_df, output_cols)

for category, df in result_dict.items():
    print(f"\ Final {category} Results ")
    print(tabulate(df, headers='keys', tablefmt='grid'))

