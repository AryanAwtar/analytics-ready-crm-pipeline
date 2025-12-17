import shutil
import os
import sys
import pandas as pd
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
        
        # Convert to dictionary for JSON response
        pipeline_stats = pm.metrics
        kpi_data = summary.to_dict(orient='records')
        sample_data = df.head(10).to_dict(orient='records') # Send top 10 rows for preview

        return JSONResponse(content={
            "status": "success",
            "message": "Pipeline executed successfully",
            "pipeline_metrics": pipeline_stats,
            "kpi_summary": kpi_data,
            "preview_data": sample_data
        })

    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    print("Starting CRM Pipeline API on http://127.0.0.1:8000...")
    uvicorn.run(app, host="127.0.0.1", port=8000)