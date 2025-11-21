import boto3
import os
import base64
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    """
    Espera en event['body']:
      {
        "bucket": "mi-bucket-123",
        # O bien provees 'key' completo:
        "key": "carpeta/sub/archivo.pdf",
        # O construyes desde 'directory' + 'filename':
        "directory": "carpeta/sub/",
        "filename": "archivo.pdf",

        # Contenido del archivo en base64
        "file_base64": "<BASE64>",
        # Opcional
        "content_type": "application/pdf"
      }
    """
    body = event.get('body', {}) or {}
    bucket = body.get('bucket')
    key = body.get('key')
    directory = body.get('directory')
    filename = body.get('filename')
    file_b64 = body.get('file_base64')
    content_type = body.get('content_type')

    if not bucket:
        return {"statusCode": 400, "error": "Falta 'bucket'."}
    if not key:
        if not (directory and filename):
            return {"statusCode": 400, "error": "Proporciona 'key' o ('directory' y 'filename')."}
        if not directory.endswith('/'):
            directory = directory + '/'
        key = directory + filename
    if not file_b64:
        return {"statusCode": 400, "error": "Falta 'file_base64'."}

    try:
        file_bytes = base64.b64decode(file_b64)
    except Exception:
        return {"statusCode": 400, "error": "El 'file_base64' no es válido."}

    try:
        s3 = boto3.client('s3')
        put_kwargs = {"Bucket": bucket, "Key": key, "Body": file_bytes}
        if content_type:
            put_kwargs["ContentType"] = content_type

        resp = s3.put_object(**put_kwargs)
        etag = resp.get('ETag', '').strip('"')

        return {
            "statusCode": 200,
            "bucket": bucket,
            "key": key,
            "size_bytes": len(file_bytes),
            "etag": etag,
            "message": "Archivo subido."
        }
    except ClientError as e:
        return {"statusCode": 400, "error": str(e)}