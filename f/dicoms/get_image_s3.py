import wmill
from minio import Minio
import base64
from io import BytesIO
from urllib.parse import urlparse
import urllib3
import ssl


s3_credentials = wmill.get_resource("f/dicoms/minio")


def parse_s3_url(s3_url: str) -> tuple[str, str]:
    """
    Parse an S3 URL into bucket name and object path.
    
    Args:
        s3_url (str): S3 URL in the format 's3://bucket-name/path/to/object'
    
    Returns:
        tuple[str, str]: (bucket_name, object_path)
    """
    parsed = urlparse(s3_url)
    if parsed.scheme != 's3':
        raise ValueError("URL must start with 's3://'")
    
    bucket_name = parsed.netloc
    # Remove leading slash from path
    object_path = parsed.path.lstrip('/')
    
    return bucket_name, object_path

def get_image_as_base64(
    image_path: str,
    minio_host: str,
    access_key: str,
    secret_key: str,
    secure: bool = True
) -> str:
    """
    Fetch an image from MinIO/S3 and return it as a base64-encoded JPEG string.
    
    Args:
        bucket_name (str): Name of the bucket containing the image
        image_path (str): Path to the image within the bucket
        minio_host (str): MinIO server host (e.g., 'minio.example.com:9000')
        access_key (str): MinIO access key
        secret_key (str): MinIO secret key
        secure (bool): Whether to use HTTPS (default True)
    
    Returns:
        str: Base64-encoded JPEG image string
    """
    # Initialize MinIO client
    client = Minio(
        minio_host,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        http_client=urllib3.PoolManager(
            cert_reqs='CERT_NONE',  # Don't verify SSL certificate
            ssl_version=ssl.PROTOCOL_TLS,
            maxsize=10,
            retries=urllib3.Retry(
                total=3,
                backoff_factor=0.2,
            )
        )
    )

    bucket_name, object_name = parse_s3_url(image_path)
    
    # Get the object data
    try:
        data = client.get_object(bucket_name, object_name)
        # Read all data into a BytesIO buffer
        buffer = BytesIO()
        for d in data.stream(32*1024):
            buffer.write(d)
        
        # Convert to base64
        base64_image = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return base64_image
        
    except Exception as e:
        raise Exception(f"Failed to fetch image: {str(e)}")
        

def main(selected_row_image):

    base64_image = get_image_as_base64(
    image_path=selected_row_image,
    minio_host=f"{s3_credentials['endPoint']}:{s3_credentials['port']}",
    access_key=s3_credentials['accessKey'],
    secret_key=s3_credentials['secretKey'],
    secure=s3_credentials['useSSL']
    )
    return base64_image
