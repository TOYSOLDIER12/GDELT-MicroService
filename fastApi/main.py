import os
import glob
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from datetime import datetime

# Config
DOWNLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../company_outputs")

# Initialize FastAPI app
app = FastAPI()

@app.get("/list")
def list_available_files():
    """
    Returns a list of available GDELT CSV files.
    """
    csv_files = sorted(
        glob.glob(os.path.join(DOWNLOAD_FOLDER, "weekly_*_news.csv")),
        key=os.path.getmtime,
        reverse=True
    )

    return {"files": [os.path.basename(f) for f in csv_files]}

@app.get("/file/{ticker}")
def get_specific_file(ticker: str):
    """
    Allows downloading a specific GDELT file by name.
    """
    filename = f"weekly_{ticker}_news.csv"
    file_path = os.path.join(DOWNLOAD_FOLDER,filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    
    return FileResponse(file_path, media_type="application/zip", filename=filename)
