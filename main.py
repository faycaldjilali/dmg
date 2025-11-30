from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import List, Optional
import requests
import pandas as pd
from datetime import datetime, date
import json
import io
import uuid
import os

app = FastAPI(title="BOAMP Data Extractor", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models
class ExtractionRequest(BaseModel):
    target_date: str
    max_records: int = 5000
    departments: List[str]

class ExtractionResponse(BaseModel):
    job_id: str
    status: str
    message: str
    total_records: Optional[int] = None
    filtered_records: Optional[int] = None

# Storage for job results
jobs = {}

# Main function to get BOAMP records
def get_all_records_for_date(target_date, max_records=5000):
    """Get all records for a specific date with all available fields"""
    url = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
    all_records = []
    offset = 0
    limit = 100
    
    while len(all_records) < max_records:
        params = {
            'order_by': 'dateparution DESC',
            "type_marche":'Travaux',
            'limit': limit,
            'offset': offset,
            
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            records = data.get('results', [])

            if not records:
                break  # No more records

            # Filter records for our target date
            target_records = [record for record in records if record.get('dateparution') == target_date]

            # If we found target records, add them
            if target_records:
                all_records.extend(target_records)

            # Check if we've moved past our target date (since we're sorting DESC)
            if records and records[-1].get('dateparution', '') < target_date:
                break

            offset += limit

            if offset > 10000:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

    return all_records

# Function to create cleaned dataframe
def create_excel_simple(records: List[dict], target_date: str):
    """Simple and robust Excel creation"""
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if isinstance(value, (list, dict)):
                cleaned_record[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                cleaned_record[key] = ''
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)

    df = pd.DataFrame(cleaned_records)

    return df



# Function to filter by departments - FIXED VERSION
def filter_by_departments(df, target_departments):



    
    
    
    
    """Filter dataframe by target departments - FIXED VERSION"""
    indices_a_conserver = []
    departements_trouves = []

    for index, row in df.iterrows():
        code_departement = row.get('code_departement', '')
        departement_trouve = None
        
        # Skip if code_departement is NaN or empty
        if pd.isna(code_departement) or code_departement == "":
            departements_trouves.append(None)
            continue
            
        # Handle different formats of code_departement
        try:
            # If it's a string that looks like a list, convert to list
            if isinstance(code_departement, str):
                # Clean the string and convert to list
                cleaned_str = code_departement.strip('[]').replace('"', '').replace("'", "")
                # Split by comma and clean each element
                dep_list = [dep.strip() for dep in cleaned_str.split(',') if dep.strip()]
            elif isinstance(code_departement, list):
                dep_list = code_departement
            else:
                # Try to convert other types to string and process
                dep_list = [str(code_departement).strip()]
            
            # Check if any department in the list matches our target departments
            for dep in dep_list:
                if dep in target_departments:
                    indices_a_conserver.append(index)
                    departement_trouve = dep
                    break
                    
        except Exception as e:
            print(f"Error processing row {index}: {e}")
            dep_list = []
        
        departements_trouves.append(departement_trouve)

    # Create filtered dataframe
    if indices_a_conserver:
        df_filtre = df.loc[indices_a_conserver].copy().reset_index(drop=True)
        df_filtre['code_departement_trouve'] = [departements_trouves[i] for i in indices_a_conserver]
    else:
        # Return empty dataframe with same columns
        df_filtre = pd.DataFrame(columns=df.columns.tolist() + ['code_departement_trouve'])
    
    return df_filtre

# Background task for data extraction
def process_extraction(job_id: str, target_date: str, max_records: int, departments: List[str]):
    try:
        jobs[job_id]["status"] = "processing"
        jobs[job_id]["message"] = "Fetching BOAMP records..."
        
        # Get all records
        all_records = get_all_records_for_date(target_date, max_records)
        
        if not all_records:
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["message"] = f"No records found for date {target_date}"
            jobs[job_id]["total_records"] = 0
            jobs[job_id]["filtered_records"] = 0
            return
        
        jobs[job_id]["total_records"] = len(all_records)
        jobs[job_id]["message"] = f"Found {len(all_records)} records. Processing..."
        
        # Create dataframe
        df = create_excel_simple(all_records, target_date)
        
        # Filter by departments
        df_filtered = filter_by_departments(df, departments)
        
        jobs[job_id]["filtered_records"] = len(df_filtered)
        jobs[job_id]["message"] = f"Processing complete. {len(df_filtered)} records after filtering."
        
        # Store results as DataFrames instead of bytes
        jobs[job_id]["full_df"] = df
        jobs[job_id]["filtered_df"] = df_filtered
        
        # Store department distribution
        if len(df_filtered) > 0 and 'code_departement_trouve' in df_filtered.columns:
            dept_counts = df_filtered['code_departement_trouve'].value_counts().to_dict()
            jobs[job_id]["department_distribution"] = dept_counts
        else:
            jobs[job_id]["department_distribution"] = {}
        
        jobs[job_id]["status"] = "completed"
        
    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["message"] = f"Error during processing: {str(e)}"
        print(f"Error in process_extraction: {e}")

# API Routes
@app.post("/extract", response_model=ExtractionResponse)
async def extract_data(request: ExtractionRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    
    jobs[job_id] = {
        "status": "started",
        "message": "Job created, starting processing...",
        "target_date": request.target_date,
        "departments": request.departments,
        "total_records": 0,
        "filtered_records": 0
    }
    
    # Start background task
    background_tasks.add_task(
        process_extraction, 
        job_id, 
        request.target_date, 
        request.max_records, 
        request.departments
    )
    
    return ExtractionResponse(
        job_id=job_id,
        status="started",
        message="Extraction started in background"
    )

@app.get("/job/{job_id}")
async def get_job_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    return {
        "job_id": job_id,
        "status": job["status"],
        "message": job["message"],
        "total_records": job.get("total_records", 0),
        "filtered_records": job.get("filtered_records", 0),
        "department_distribution": job.get("department_distribution", {})
    }

@app.get("/download/{job_id}/full")
async def download_full_data(job_id: str):
    if job_id not in jobs or jobs[job_id]["status"] != "completed":
        raise HTTPException(status_code=404, detail="Data not available")
    
    if "full_df" not in jobs[job_id]:
        raise HTTPException(status_code=404, detail="Full data not available")
    
    target_date = jobs[job_id]["target_date"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"BOAMP_Full_Results_{target_date}_{timestamp}.xlsx"
    
    # Create Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        jobs[job_id]["full_df"].to_excel(writer, index=False, sheet_name='BOAMP_Data')
    output.seek(0)
    
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.get("/download/{job_id}/filtered")
async def download_filtered_data(job_id: str):
    if job_id not in jobs or jobs[job_id]["status"] != "completed":
        raise HTTPException(status_code=404, detail="Data not available")
    
    if "filtered_df" not in jobs[job_id]:
        raise HTTPException(status_code=404, detail="Filtered data not available")
    
    target_date = jobs[job_id]["target_date"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"BOAMP_Filtered_Results_{target_date}_{timestamp}.xlsx"
    
    # Create Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        jobs[job_id]["filtered_df"].to_excel(writer, index=False, sheet_name='BOAMP_Filtered')
    output.seek(0)
    
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

# Serve static files
from fastapi.staticfiles import StaticFiles
app.mount("/", StaticFiles(directory="static", html=True), name="static")
