import sys
import os
import pandas as pd

# Add the project root to the system path to allow importing from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src import ingestion, cleaning, enrichment, entity_resolution, pricing, metrics

def run():
    # Setup paths
    base_dir = os.path.dirname(os.path.dirname(__file__))
    input_file = os.path.join(base_dir, 'data', 'raw', 'crm_raw_data.csv')
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    reports_dir = os.path.join(base_dir, 'reports')
    
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)
    
    # Initialize Metrics
    pm = metrics.PipelineMetrics()
    
    # --- Step 1: Ingestion ---
    try:
        df = ingestion.load_data(input_file)
        df = ingestion.normalize_schema(df)
        pm.log_stage("Ingestion", df)
    except Exception as e:
        print(f"Pipeline failed at Ingestion: {e}")
        return

    # --- Step 2: Cleaning ---
    df = cleaning.remove_exact_duplicates(df)
    df = cleaning.handle_missing_values(df)
    df = cleaning.normalize_text_fields(df)
    pm.log_stage("Cleaning", df)

    # --- Step 3: Enrichment ---
    df = enrichment.standardize_state_names(df)
    df = enrichment.enrich_regions(df)
    df = enrichment.fill_missing_industry_from_keywords(df)
    pm.log_stage("Enrichment", df)

    # --- Step 4: Entity Resolution ---
    df = entity_resolution.fuzzy_match_companies(df)
    df = entity_resolution.deduplicate_entities(df)
    pm.log_stage("Entity Resolution", df)

    # --- Step 5: Pricing & Segmentation ---
    df = pricing.segment_customers(df)
    df = pricing.calculate_scoring(df)
    pm.log_stage("Pricing & Scoring", df)

    # --- Step 6: Exports ---
    output_file = os.path.join(processed_dir, 'crm_analytics_ready.csv')
    df.to_csv(output_file, index=False)
    print(f"Processed data saved to {output_file}")
    
    # Save Metrics
    pm.save_metrics(os.path.join(reports_dir, 'pipeline_metrics.json'))
    
    # Summary Report
    summary = df.groupby(['segment', 'deal_stage']).agg(
        total_revenue=('revenue', 'sum'),
        avg_score=('lead_score', 'mean'),
        count=('id', 'count')
    ).reset_index()
    
    summary_file = os.path.join(reports_dir, 'kpi_summary.csv')
    summary.to_csv(summary_file, index=False)
    print("Pipeline Execution Completed Successfully.")

if __name__ == "__main__":
    run()