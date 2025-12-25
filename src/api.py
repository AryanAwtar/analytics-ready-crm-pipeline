import shutil
import os
import sys
import pandas as pd
import numpy as np
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Add src to path to import existing modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import your existing pipeline modules
import ingestion
import cleaning
import enrichment
import entity_resolution
import pricing
import metrics

app = FastAPI(title="CRM Pipeline API")

# Allow CORS for frontend connection
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "CRM Pipeline API is running. POST to /upload-and-process to use."}

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'processed')
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

@app.post("/upload-and-process")
async def upload_file(file: UploadFile = File(...)):
    """
    1. Uploads the raw CRM CSV.
    2. Runs the full ETL pipeline.
    3. Returns the KPI summary and sample data.
    """
    try:
        # 1. Save the uploaded file
        file_location = os.path.join(DATA_DIR, "crm_raw_data.csv")
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # 2. Initialize Metrics
        pm = metrics.PipelineMetrics()

        # 3. Run ETL Stages (Reusing your logic)
        # Ingestion
        df = ingestion.load_data(file_location)
        df = ingestion.normalize_schema(df)
        pm.log_stage("Ingestion", df)

        # Cleaning
        df = cleaning.remove_exact_duplicates(df)
        df = cleaning.handle_missing_values(df)
        df = cleaning.normalize_text_fields(df)
        pm.log_stage("Cleaning", df)

        # Enrichment
        df = enrichment.standardize_state_names(df)
        df = enrichment.enrich_regions(df)
        df = enrichment.fill_missing_industry_from_keywords(df)
        pm.log_stage("Enrichment", df)

        # Entity Resolution
        df = entity_resolution.fuzzy_match_companies(df)
        df = entity_resolution.deduplicate_entities(df)
        pm.log_stage("Entity Resolution", df)

        # Pricing & Segmentation
        df = pricing.segment_customers(df)
        df = pricing.calculate_scoring(df)
        pm.log_stage("Pricing & Scoring", df)

        # 4. Generate Summary for Frontend
        summary = df.groupby(['segment', 'deal_stage']).agg(
            total_revenue=('revenue', 'sum'),
            avg_score=('lead_score', 'mean'),
            count=('id', 'count')
        ).reset_index()
        
        # --- FIX: Robust serialization handling ---
        # 1. Replace Infinite values with None
        # 2. Replace NaN values with None
        # 3. Replace NaT (Time) values with None
        # 4. Convert Timestamp objects to strings
        
        def clean_for_json(data_frame):
            # Create a copy to avoid SettingWithCopy warnings
            df_out = data_frame.copy()
            
            # Convert datetime objects to strings (ISO format)
            # This handles the "Object of type Timestamp is not JSON serializable" error
            for col in df_out.columns:
                if pd.api.types.is_datetime64_any_dtype(df_out[col]):
                    # Convert to string, replacing NaT with the string "NaT" first
                    df_out[col] = df_out[col].astype(str)
                    # Now replace the string "NaT" with None
                    df_out[col] = df_out[col].replace({'NaT': None, 'nan': None})

            # Replace Infinity
            df_out.replace([np.inf, -np.inf], None, inplace=True)
            
            # Replace NaN and NaT (for non-datetime columns)
            # Note: where(pd.notnull(df_out), None) replaces NaN/NaT with None
            df_out = df_out.where(pd.notnull(df_out), None)
            
            return df_out

        df_clean = clean_for_json(df)
        summary_clean = clean_for_json(summary)
        
        # Convert to dictionary for JSON response
        pipeline_stats = pm.metrics
        kpi_data = summary_clean.to_dict(orient='records')
        sample_data = df_clean.head(10).to_dict(orient='records') # Send top 10 rows for preview

        return JSONResponse(content={
            "status": "success",
            "message": "Pipeline executed successfully",
            "pipeline_metrics": pipeline_stats,
            "kpi_summary": kpi_data,
            "preview_data": sample_data
        })

    except Exception as e:
        # Print the full error to the terminal for debugging
        print(f"ERROR: {str(e)}")
        # Return a 200 OK with error status so frontend can display the message cleanly
        # instead of a generic 500 browser error.
        return JSONResponse(content={
            "status": "error", 
            "message": f"Server Error: {str(e)}"
        })

if __name__ == "__main__":
    import uvicorn
    print("Starting CRM Pipeline API on http://127.0.0.1:8000...")
    uvicorn.run(app, host="127.0.0.1", port=8000)