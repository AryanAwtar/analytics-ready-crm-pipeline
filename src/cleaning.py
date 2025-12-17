import pandas as pd
import numpy as np

def handle_missing_values(df):
    """
    Fills missing values with defaults and drops rows with critical missing data.
    """
    print("Handling missing values...")
    
    # Critical: If company name is missing, mark as 'Unknown Entity'
    df['company_name'] = df['company_name'].fillna('Unknown Entity')
    
    # Fill numeric missings
    df['revenue'] = df['revenue'].fillna(0)
    df['employee_count'] = df['employee_count'].fillna(0)
    
    # Fill categorical missings
    df['industry'] = df['industry'].fillna('Other')
    df['deal_stage'] = df['deal_stage'].fillna('Unqualified')
    df['owner'] = df['owner'].fillna('Unassigned')
    
    return df

def normalize_text_fields(df):
    """
    Standardizes text casing and whitespace.
    """
    print("Normalizing text fields...")
    
    text_cols = ['company_name', 'contact_name', 'city', 'state', 'industry', 'deal_stage', 'owner']
    
    for col in text_cols:
        if col in df.columns:
            # Title case, strip whitespace
            df[col] = df[col].astype(str).str.strip().str.title()
            
    # Lowercase email for consistency
    if 'email' in df.columns:
        df['email'] = df['email'].astype(str).str.lower().str.strip().replace('nan', np.nan)
        
    return df

def remove_exact_duplicates(df):
    """
    Removes exact row duplicates.
    """
    initial_count = len(df)
    df = df.drop_duplicates()
    removed = initial_count - len(df)
    if removed > 0:
        print(f"Removed {removed} exact duplicate rows.")
    return df