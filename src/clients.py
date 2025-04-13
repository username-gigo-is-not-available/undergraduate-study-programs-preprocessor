from minio import Minio

from src.config import Config


class MinioClient:
    _instance: 'MinioClient' = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.client = Minio(
                endpoint=Config.MINIO_ENDPOINT_URL,
                access_key=Config.MINIO_ACCESS_KEY,
                secret_key=Config.MINIO_SECRET_KEY,
                secure=False,
            )
        return cls._instance
