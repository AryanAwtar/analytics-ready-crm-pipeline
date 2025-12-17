import pandas as pd
import json
from datetime import datetime

class PipelineMetrics:
    def __init__(self):
        self.metrics = {
            'timestamp': datetime.now().isoformat(),
            'stages': []
        }
        
    def log_stage(self, stage_name, df):
        """
        Logs row counts and basic stats for a specific pipeline stage.
        """
        stage_info = {
            'stage': stage_name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'missing_values_total': int(df.isnull().sum().sum())
        }
        
        # Special metric for specific stages
        if 'revenue' in df.columns:
            stage_info['total_pipeline_value'] = float(df['revenue'].sum())
            
        self.metrics['stages'].append(stage_info)
        print(f"--- [METRICS] {stage_name}: {len(df)} rows ---")

    def save_metrics(self, filepath):
        """
        Saves metrics to a JSON file.
        """
        with open(filepath, 'w') as f:
            json.dump(self.metrics, f, indent=4)
        print(f"Metrics saved to {filepath}")
        
    def generate_summary_df(self):
        return pd.DataFrame(self.metrics['stages'])