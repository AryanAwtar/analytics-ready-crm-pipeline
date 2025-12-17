import pandas as pd
from difflib import get_close_matches

def fuzzy_match_companies(df, threshold=0.85):
    """
    Identifies and standardizes similar company names using basic fuzzy matching.
    Simple implementation: Finds canonical name for similar groups.
    """
    print("Running entity resolution (fuzzy matching)...")
    
    unique_companies = df['company_name'].unique().tolist()
    canonical_map = {}
    
    # Simple logic: Sort by length (assume longer name is more descriptive or first occurrence)
    unique_companies.sort(key=len, reverse=True)
    
    processed = set()
    
    for company in unique_companies:
        if company in processed:
            continue
            
        # Find close matches in the remaining list
        matches = get_close_matches(company, unique_companies, n=5, cutoff=threshold)
        
        # Map all matches to the current company (the 'canonical' one)
        for match in matches:
            if match not in processed:
                canonical_map[match] = company
                processed.add(match)
                
    df['company_name_normalized'] = df['company_name'].map(canonical_map)
    return df

def deduplicate_entities(df):
    """
    Aggregates data based on the normalized company name and contact info.
    Keeps the most recent or most complete record.
    """
    print("Consolidating duplicate entities...")
    
    # Logic: Sort by Close Date (descending) or ID to keep latest info
    if 'close_date' in df.columns:
        df['close_date'] = pd.to_datetime(df['close_date'], errors='coerce')
        df = df.sort_values(by=['company_name_normalized', 'close_date'], ascending=[True, False])
    
    # We keep the first occurrence after sorting (Latest deal) 
    # But in a real CRM, you might want to aggregate revenue. 
    # Here we assume we want unique Company profiles.
    
    # Group by normalized company and take the first (latest)
    # We drop the original company_name in favor of normalized
    df_deduped = df.drop_duplicates(subset=['company_name_normalized', 'email'], keep='first').copy()
    
    # Restore column name
    df_deduped['original_company_name'] = df_deduped['company_name']
    df_deduped['company_name'] = df_deduped['company_name_normalized']
    df_deduped.drop(columns=['company_name_normalized'], inplace=True)
    
    return df_deduped