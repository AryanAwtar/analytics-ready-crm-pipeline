import pandas as pd
import numpy as np

def calculate_scoring(df):
    """
    Calculates a simple lead score based on available data.
    """
    print("Calculating lead scores...")
    
    df['lead_score'] = 0
    
    # Points for revenue
    df.loc[df['revenue'] > 1000000, 'lead_score'] += 30
    df.loc[(df['revenue'] <= 1000000) & (df['revenue'] > 0), 'lead_score'] += 10
    
    # Points for employees
    df.loc[df['employee_count'] > 100, 'lead_score'] += 20
    
    # Points for having contact info
    df.loc[df['email'].notna(), 'lead_score'] += 10
    df.loc[df['phone'].notna(), 'lead_score'] += 10
    
    # Points for Deal Stage
    stage_weights = {
        'Closed Won': 50,
        'Negotiation': 40,
        'Proposal': 30,
        'Qualified': 20,
        'Discovery': 10,
        'Lead': 5
    }
    
    df['lead_score'] += df['deal_stage'].map(stage_weights).fillna(0)
    
    return df

def segment_customers(df):
    """
    Segments customers into business tiers based on revenue and employees.
    """
    print("Segmenting customers...")
    
    conditions = [
        (df['revenue'] >= 5000000) | (df['employee_count'] >= 500),
        (df['revenue'] >= 1000000) | (df['employee_count'] >= 100),
        (df['revenue'] > 0)
    ]
    choices = ['Enterprise', 'Mid-Market', 'SMB']
    
    df['segment'] = np.select(conditions, choices, default='Unknown')
    
    return df