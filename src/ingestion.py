import pandas as pd
import os

def load_data(filepath):
    """
    Loads raw data from CSV or JSON.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    print(f"Loading data from {filepath}...")
    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    elif filepath.endswith('.json'):
        df = pd.read_json(filepath)
    else:
        raise ValueError("Unsupported file format. Use CSV or JSON.")
    
    return df

def normalize_schema(df):
    """
    Standardizes column names to snake_case and ensures essential columns exist.
    """
    print("Normalizing schema...")
    
    # Clean column names: lowercase, strip spaces, replace spaces/special chars with underscores
    df.columns = df.columns.str.lower().str.strip().str.replace(r'[^\w\s]', '', regex=True).str.replace(' ', '_')
    
    # Define expected core columns map (flexible mapping)
    column_mapping = {
        'company': 'company_name',
        'organization': 'company_name',
        'contact': 'contact_name',
        'person': 'contact_name',
        'zip': 'zip_code',
        'postal_code': 'zip_code',
        'employees': 'employee_count',
        'annual_revenue': 'revenue'
    }
    
    df.rename(columns=column_mapping, inplace=True)
    
    # Ensure ID column exists, create if not
    if 'id' not in df.columns:
        df['id'] = range(1, len(df) + 1)
        
    return df