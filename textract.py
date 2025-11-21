import json
import os
import boto3

textract = boto3.client("textract")

DEFAULT_BUCKET = os.getenv("BUCKET_NAME")
DEFAULT_DOCUMENT = os.getenv("DOCUMENT_NAME", "AT-998923-James-Lebron.pdf")

QUERIES = [
    {"Text": "FROM",  "Alias": "FromQuery",  "Pages": ["1"]},
    {"Text": "TO",    "Alias": "ToQuery",    "Pages": ["1"]},
    {"Text": "TOTAL", "Alias": "TotalQuery", "Pages": ["1"]},
]

def _run_queries(bucket: str, document: str, queries):
    """
    Ejecuta AnalyzeDocument con FeatureTypes=['QUERIES'] y retorna el payload.
    """
    return textract.analyze_document(
        Document={"S3Object": {"Bucket": bucket, "Name": document}},
        FeatureTypes=["QUERIES"],
        QueriesConfig={"Queries": queries},
    )

def _answers_by_alias(textract_response):
    """
    Extrae las respuestas de Textract organizadas por alias de consulta.
    - Busca bloques de tipo QUERY y QUERY_RESULT
    - Une cada QUERY con su QUERY_RESULT por el Relationship
    """
    blocks = {b["Id"]: b for b in textract_response.get("Blocks", [])}

    # Construir índice de relaciones
    results = {}
    for block in blocks.values():
        if block.get("BlockType") == "QUERY":
            alias = (block.get("Query", {}) or {}).get("Alias")
            if not alias:
                continue

            # Los QUERY tienen relaciones hacia QUERY_RESULT
            related_ids = []
            for rel in block.get("Relationships", []):
                if rel.get("Type") == "ANSWER":
                    related_ids.extend(rel.get("Ids", []))

            # Puede haber múltiples respuestas, tomamos la mejor (Confidence más alta)
            best_text = ""
            best_conf = -1.0
            for rid in related_ids:
                ans = blocks.get(rid, {})
                if ans.get("BlockType") != "QUERY_RESULT":
                    continue
                text = ans.get("Text") or ""
                conf = ans.get("Confidence", 0.0)
                if conf > best_conf and text.strip():
                    best_text, best_conf = text.strip(), conf

            results[alias] = {"text": best_text, "confidence": best_conf}

    return results

def lambda_handler(event, context):
    """
    event opcional:
    {
      "bucket": "mi-bucket",
      "document": "ruta/archivo.pdf",
      "queries": [
        {"Text": "FROM", "Alias": "FromQuery", "Pages": ["1"]},
        ...
      ]
    }
    """
    bucket = (event or {}).get("bucket") or DEFAULT_BUCKET
    document = (event or {}).get("document") or DEFAULT_DOCUMENT
    queries = (event or {}).get("queries") or QUERIES

    try:
        response = _run_queries(bucket, document, queries)
        answers = _answers_by_alias(response)

        # Puedes mapear a llaves “amigables” si conoces los alias
        result = {
            "from": answers.get("FromQuery", {}).get("text", ""),
            "to": answers.get("ToQuery", {}).get("text", ""),
            "total": answers.get("TotalQuery", {}).get("text", ""),
            "raw": answers,  # por si quieres todo el detalle
        }

        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": str(e), "bucket": bucket, "document": document},
                ensure_ascii=False,
            ),
        }
