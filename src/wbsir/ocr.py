import easyocr
from pdf2image import convert_from_path
import numpy as np
from tqdm import tqdm


def extract_text_from_pdf(pdf_path: str, language: str) -> str:
    """
    Extract text from a given PDF file using OCR.

    Parameters:
        pdf_path (str): The path to the input PDF file.
        language (str): The language code for OCR (e.g., 'en' for English).

    Returns:
        str: The extracted text from the PDF.
    """
    reader = easyocr.Reader([language])

    images = convert_from_path(pdf_path)

    ocr_text = ""
    for image in tqdm(images, desc="Processing PDF pages"):
        ocr_result = reader.readtext(np.array(image), detail=0)
        ocr_text += "\n".join(ocr_result) + "\n"

    return ocr_text


if __name__ == "__main__":
    pdf_file_path = "./data/AC001PART001.pdf"
    lang = "bn"
    extracted_text = extract_text_from_pdf(pdf_file_path, lang)
    print(extracted_text)
