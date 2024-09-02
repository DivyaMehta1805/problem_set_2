import pandas as pd
import random
from datetime import datetime, timedelta

def generate_date_between(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_event_attributes():
    event_urls = [f"event_{i}" for i in range(1, 101)]
    cities = ['Europe']
    countries = ['USA', 'UK', 'France', 'Japan', 'Australia', 'Germany']
    industries = ['Tech', 'Software', 'Finance', 'Healthcare', 'Biotech', 'Medtech', 'Green Energy']
    event_types = ['In-person', 'Virtual', 'Hybrid']
    continents = ['Europe']

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2026, 12, 31)

    data = []
    for url in event_urls:
        event_date = generate_date_between(start_date, end_date)
        data.extend([
            (url, 'event_start_date', event_date.strftime('%Y-%m-%d')),
            (url, 'event_city', random.choice(cities)),
            (url, 'event_country', random.choice(countries)),
            (url, 'event_industry', random.choice(industries)),
            (url, 'event_type', random.choice(event_types)),
            (url, 'event_continent', random.choice(continents)),
            (url, 'event_name', f"Conference {random.randint(1, 1000)}")
        ])

    return pd.DataFrame(data, columns=['event_url', 'attribute', 'value'])

def generate_company_attributes():
    company_urls = [f"company_{i}" for i in range(1, 101)]
    industries = ['Tech', 'Software', 'Finance', 'Healthcare', 'Medtech', 'Biotech', 'Green Energy']
    countries = ['USA', 'UK', 'France', 'Japan', 'Germany', 'Canada', 'Australia']
    employee_counts = ['1-50', '51-200', '201-1000', '1001-5000', '5000+']

    data = []
    for url in company_urls:
        data.extend([
            (url, 'company_name', f"Company {random.randint(1, 1000)}"),
            (url, 'company_industry', random.choice(industries)),
            (url, 'company_country', random.choice(countries)),
            (url, 'company_employee_count', random.choice(employee_counts)),
(url, 'company_revenue', random.randint(100, 10000))  # Revenue in whole dollars
        ])

    return pd.DataFrame(data, columns=['company_url', 'attribute', 'value'])

def generate_people_attributes():
    person_ids = [f"person_{i}" for i in range(1, 101)]
    first_names = [ 'Jane', 'Michael', 'Emily', 'David', 'Sarah']
    last_names = ['Smith', 'Johnson', 'Brown', 'Taylor', 'Anderson']
    seniorities = ['Junior', 'Mid-level', 'Senior', 'Director', 'VP', 'C-level']
    departments = ['Sales', 'Marketing', 'Engineering', 'HR', 'Finance']

    data = []
    for pid in person_ids:
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        data.extend([
            (pid, 'person_first_name', first_name),
            (pid, 'person_last_name', last_name),
            (pid, 'person_email', f"{first_name.lower()}.{last_name.lower()}@example.com"),
            (pid, 'person_seniority', random.choice(seniorities)),
            (pid, 'person_department', random.choice(departments))
        ])

    return pd.DataFrame(data, columns=['person_id', 'attribute', 'value'])

# Generate the sample data
events_df = generate_event_attributes()
companies_df = generate_company_attributes()
company_employee_info_df = generate_people_attributes()

print("Sample data generated successfully.")
