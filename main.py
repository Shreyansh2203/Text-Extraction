
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Union
import base64
import io
import pdfplumber
import re
try:
    import pytesseract
    from pdf2image import convert_from_bytes
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("OCR dependencies not available. Install pytesseract and pdf2image.")

app = FastAPI()

# --- Data Models ---

# Model for a single attachment in the Graph API 'value' list
class GraphAttachment(BaseModel):
    name: Optional[str] = Field(None, description="File name")
    contentType: Optional[str] = Field(None, description="MIME type")
    contentBytes: str = Field(..., description="Base64 encoded content")
    # Allow other graph fields to be present without validation errors
    class Config:
        extra = "ignore"

# Model for the root Graph API response
class GraphPayload(BaseModel):
    value: List[GraphAttachment]
    # Optional fields for API configuration (mixed into the payload if needed)
    extraction_rules: Optional[Dict[str, str]] = None
    table_settings: Optional[Dict[str, Any]] = None
    class Config:
        extra = "ignore"

# Legacy model for backward compatibility (single file)
class SingleFilePayload(BaseModel):
    ContentType: str = Field(..., alias="contentType")
    contentBytes: str
    extraction_rules: Optional[Dict[str, str]] = None
    table_settings: Optional[Dict[str, Any]] = None
    
    class Config:
        populate_by_field_name = True  # Allows both 'contentType' and 'ContentType'

# --- Core Logic ---

GENERIC_PATTERN = re.compile(r"^([^:\n]+):\s*(.+)$", re.MULTILINE)

def parse_pdf_content(pdf, pdf_bytes: bytes = None, rules: Optional[Dict[str, str]] = None, table_settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    extracted_data = {
        "text_fields": {},
        "tables": [],
        "raw_text": ""
    }
    
    all_text = ""
    for i, page in enumerate(pdf.pages):
        page_text = page.extract_text()
        
        # Hybrid Strategy: If page text is empty, try OCR for this page
        if not page_text and OCR_AVAILABLE and pdf_bytes:
            try:
                # Convert specific page to image (1-based index)
                page_images = convert_from_bytes(pdf_bytes, first_page=i+1, last_page=i+1, dpi=300)
                if page_images:
                    page_text = pytesseract.image_to_string(page_images[0])
            except Exception as e:
                print(f"OCR failed for page {i+1}: {e}")
                
        if page_text:
            all_text += page_text + "\n"
    
    extracted_data["raw_text"] = all_text
    
    # Use first page for legacy rule extraction or use all data? 
    # Usually rules are for the whole doc, but let's stick to using all_text for regex search.
    text = all_text
    first_page = pdf.pages[0] # Keep reference for table extraction if needed
    
    # Text Extraction Logic
    if rules:
        # Mode 1: Config Driven
        for key, pattern in rules.items():
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                value = match.group(1).strip() if match.groups() else match.group(0).strip()
                extracted_data["text_fields"][key] = value
    else:
        # Mode 2: Auto-Discovery
        matches = GENERIC_PATTERN.finditer(text)
        for match in matches:
            key = match.group(1).strip()
            value = match.group(2).strip()
            if len(key) < 50 and len(value) > 0:
                extracted_data["text_fields"][key] = value

    # Table Extraction (Optimization: Only if requested)
    if table_settings:
        tables = first_page.extract_tables(table_settings)
    
        if not tables:
            fallback_settings = {
                "vertical_strategy": "text",
                "horizontal_strategy": "text",
                "intersection_y_tolerance": 10
            }
            fallback_settings.update(table_settings)
            tables = first_page.extract_tables(fallback_settings)
        
        if tables:
            for i, table in enumerate(tables):
                if len(table) > 1:
                    headers = [str(h).replace('\n', ' ').strip().lower() if h else f"col_{j}" for j, h in enumerate(table[0])]
                    table_data = []
                    for row in table[1:]:
                        row_dict = {}
                        if not any(row): continue
                        for j, cell in enumerate(row):
                            if j < len(headers):
                                clean_cell = str(cell).replace('\n', ' ').strip() if cell else ""
                                row_dict[headers[j]] = clean_cell
                        if any(row_dict.values()):
                            table_data.append(row_dict)
                    if table_data:
                        extracted_data["tables"].append(table_data)
                else:
                    extracted_data["tables"].append(table)

    return extracted_data

def process_single_pdf(content_bytes: str, rules=None, table_settings=None):
    try:
        pdf_bytes = base64.b64decode(content_bytes)
        
        extracted_data = {"error": "Unknown error"}
        
        with io.BytesIO(pdf_bytes) as f:
            with pdfplumber.open(f) as pdf:
                if not pdf.pages:
                    return {"error": "PDF has no pages"}
                if not pdf.pages:
                    return {"error": "PDF has no pages"}
                # Pass pdf_bytes for OCR fallback
                extracted_data = parse_pdf_content(pdf, pdf_bytes, rules, table_settings)
        
        return extracted_data
    except Exception as e:
        return {"error": str(e)}

# --- API Endpoint ---

@app.post("/extract_text")
async def extract_text(payload: Union[GraphPayload, SingleFilePayload, List[GraphAttachment]]):
    try:
        results = []
        attachments = []

        # Normalize input to a list of attachments
        if isinstance(payload, list):
            attachments = payload
        elif isinstance(payload, GraphPayload):
            attachments = payload.value
        elif isinstance(payload, SingleFilePayload):
            # Convert single file legacy payload to list item for uniform processing
            attachments = [GraphAttachment(
                contentType=payload.ContentType,
                contentBytes=payload.contentBytes
            )]

        for attachment in attachments:
            # Filter for PDFs (or assume PDF if metadata is missing but contentBytes exists)
            is_pdf = False
            if attachment.contentType and "pdf" in attachment.contentType.lower():
                is_pdf = True
            elif attachment.name and attachment.name.lower().endswith(".pdf"):
                is_pdf = True
            elif not attachment.name and not attachment.contentType and attachment.contentBytes:
                # Fallback: If no metadata, try to process it anyway
                is_pdf = True

            if is_pdf:
                fname = attachment.name or "unknown_file"
                print(f"Processing PDF: {fname}")
                # Pass explicit None for rules/settings if not available in this scope, 
                # or extract them if the payload type supports it (SingleFile/GraphPayload could have them, but List doesn't natively)
                # For simplicity in list mode, we use default settings.
                data = process_single_pdf(attachment.contentBytes)
                
                if "error" in data:
                    results.append({"raw_text": f"Error: {data['error']}"})
                else:
                    # User requested "raw_text": value format, implying a JSON object
                    results.append({"raw_text": data.get("raw_text", "").strip()})
            else:
                results.append({"raw_text": "Error: Not a PDF"})
        
        # If it was a single file request (SingleFilePayload), return just the single result
        if isinstance(payload, SingleFilePayload) and len(results) == 1:
             return results[0]

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")