from fastapi import FastAPI, UploadFile, File, HTTPException
import pdfplumber
import io
import json
import base64
from pydantic import BaseModel
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0

app = FastAPI(title="PDF Text Extractor API")

def extract_text_from_pdf_stream(pdf_stream):
    pages_data = []
    with pdfplumber.open(pdf_stream) as pdf:
        for i, page in enumerate(pdf.pages):
            page_num = i + 1
            text = page.extract_text()
            if text:
                try:
                    lang = detect(text)
                except:
                    lang = "unknown"
                    
                pages_data.append({
                    "language": lang,
                    "pagenumber": page_num,
                    "raw_text": text,
                })
    return pages_data

@app.post("/extract_file")
async def extract_pdf_endpoint(file: UploadFile = File(...)):
   
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")

    try:
        contents = await file.read()
        pdf_stream = io.BytesIO(contents)
        
        pages_data = extract_text_from_pdf_stream(pdf_stream)
        
        return {"data": pages_data}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing PDF: {str(e)}")

class PDFRequest(BaseModel):
    contentType: str
    contentBytes: str

@app.post("/extract_json")
async def extract_from_bytes_endpoint(request: PDFRequest):
    if request.contentType.upper() != "PDF":
        raise HTTPException(status_code=400, detail="Only PDF content type is supported.")

    try:
        pdf_bytes = base64.b64decode(request.contentBytes)
        pdf_stream = io.BytesIO(pdf_bytes)
        
        pages_data = extract_text_from_pdf_stream(pdf_stream)
        
        return {"data": pages_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing base64 PDF: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
