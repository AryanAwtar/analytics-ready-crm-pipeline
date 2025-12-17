import pandas as pd

def standardize_state_names(df):
    """
    Converts full state names to abbreviations (US-centric example).
    """
    print("Standardizing state names...")
    
    state_map = {
        'New York': 'NY',
        'California': 'CA',
        'Texas': 'TX',
        'Illinois': 'IL',
        'Florida': 'FL',
        'Washington': 'WA',
        # Add more as needed
    }
    
    if 'state' in df.columns:
        df['state'] = df['state'].replace(state_map)
        
    return df

def enrich_regions(df):
    """
    Enriches dataset with a 'Region' column based on State.
    """
    print("Enriching regions...")
    
    regions = {
        'Northeast': ['NY', 'MA', 'PA', 'NJ', 'CT'],
        'West': ['CA', 'WA', 'OR', 'NV', 'AZ'],
        'South': ['TX', 'FL', 'GA', 'NC', 'TN'],
        'Midwest': ['IL', 'OH', 'MI', 'WI', 'MN']
    }
    
    # Reverse map for easier lookup
    state_to_region = {state: region for region, states in regions.items() for state in states}
    
    df['region'] = df['state'].map(state_to_region).fillna('Other')
    return df

def fill_missing_industry_from_keywords(df):
    """
    Attempts to guess industry from company name if missing or 'Other'.
    """
    print("Enriching missing industries...")
    
    keywords = {
        'Tech': 'Technology',
        'Soft': 'Technology',
        'Data': 'Technology',
        'Logistics': 'Logistics',
        'Transport': 'Logistics',
        'Finance': 'Finance',
        'Capital': 'Finance',
        'Bank': 'Finance',
        'Retail': 'Retail',
        'Shop': 'Retail',
        'Mfg': 'Manufacturing',
        'Corp': 'Corporate' # Fallback
    }
    
    def guess_industry(row):
        if row['industry'] != 'Other':
            return row['industry']
        
        name = str(row['company_name'])
        for key, val in keywords.items():
            if key.lower() in name.lower():
                return val
        return 'Other'

    df['industry'] = df.apply(guess_industry, axis=1)
    return df