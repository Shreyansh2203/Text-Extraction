"""
FastAPI PDF Text Extraction Service
Returns only clean raw text from PDFs
"""

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Union
import base64
import io
import re
import pdfplumber

try:
    import pytesseract
    from pdf2image import convert_from_bytes
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

app = FastAPI(title="PDF Text Extractor", version="2.0")


class Attachment(BaseModel):
    name: Optional[str] = None
    contentType: Optional[str] = None
    contentBytes: str
    
    class Config:
        extra = "ignore"


class GraphPayload(BaseModel):
    value: List[Attachment]
    
    class Config:
        extra = "ignore"


class SingleFilePayload(BaseModel):
    contentType: Optional[str] = Field(None, alias="ContentType")
    contentBytes: str
    
    class Config:
        populate_by_name = True
        extra = "ignore"


class PageData(BaseModel):
    pagenumber: int
    raw_text: str

class ExtractionResponse(BaseModel):
    data: List[PageData]

class PDFPayload(BaseModel):
    contentType: Optional[str] = "PDF"
    contentBytes: str

    class Config:
        extra = "ignore"


def clean_text(text: str) -> str:
    if not text:
        return ""
    lines = text.split('\n')
    cleaned = []
    prev_empty = False
    
    for line in lines:
        line = line.rstrip()
        line = re.sub(r' {3,}', ' ', line)  
        line = line.strip()
        is_empty = not line
        
        if is_empty:
            if not prev_empty:
                cleaned.append('')
            prev_empty = True
        else:
            cleaned.append(line)
            prev_empty = False
    
    return '\n'.join(cleaned).strip()


def extract_pages_from_pdf(pdf_bytes: bytes) -> List[dict]:
    pages_data = []
    
    with io.BytesIO(pdf_bytes) as f:
        with pdfplumber.open(f) as pdf:
            for i, page in enumerate(pdf.pages):
                page_text = page.extract_text(
                    x_tolerance=3,
                    y_tolerance=3,
                    layout=True,
                    x_density=7.25,
                    y_density=13
                )
                
                if not page_text:
                    page_text = page.extract_text() or ""
                
                if not page_text and OCR_AVAILABLE:
                     try:
                        images = convert_from_bytes(pdf_bytes, first_page=i+1, last_page=i+1, dpi=300)
                        if images:
                            page_text = pytesseract.image_to_string(images[0])
                     except Exception:
                        pass

                cleaned_text = clean_text(page_text)
                
                pages_data.append({
                    "pagenumber": i + 1,
                    "raw_text": cleaned_text
                })
    
    return pages_data


def process_pdf(content_bytes: str) -> dict:
    try:
        pdf_bytes = base64.b64decode(content_bytes)
        pages = extract_pages_from_pdf(pdf_bytes)
        return {"data": pages}
    except Exception as e:
        raise e



@app.post("/extract_text", response_model=ExtractionResponse)
async def extract_text(payload: PDFPayload):
    try:
        if payload.contentType and "pdf" not in payload.contentType.lower():
             pass 
        
        result = process_pdf(payload.contentBytes)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload", response_model=ExtractionResponse)
async def upload_pdf(file: UploadFile = File(...)):
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files allowed")
    
    try:
        pdf_bytes = await file.read()
        pages = extract_pages_from_pdf(pdf_bytes)
        return {"data": pages}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
