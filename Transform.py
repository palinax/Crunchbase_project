import pandas as pd
from datetime import datetime

#  Convert a column in DataFrame to datetime format, coercing errors

def process_dates(df, date_column):
    df[date_column] = pd.to_datetime(df[date_column], errors = 'coerce')
    return df 

# Calculate the age in years from a datetime column to the current year

def calculate_age(df, date_column):
    current_year = datetime.now().year 
    df["age"] = current_year - df[date_column].dt.year
    return df

# Filter a DataFrame based on a query string

def filter_and_select(df, condition, columns):
    filtered_df = df.query(condition)
    return filtered_df[columns]

# Merge two DataFrames on a specified key

def merge_dataframes(df1, df2, on_key, how_type='inner'):
    return pd.merge(df1, df2, on=on_key, how=how_type)

# Ad new columns 
    
def add_new_columns(df, iso_codes_column, category_list_column):
    df['has_a_lead_investor'] = df['lead_investor_uuids'].notna().astype(int)
    df['first_category'] = df[category_list_column].str.split(',').str[0]
    df['country'] = df[iso_codes_column].apply(lambda x: pycountry.countries.get(alpha_3=x).name if pycountry.countries.get(alpha_3=x) else None)
    df['num_exits'] = df['num_exits'].fillna(0)
    return df