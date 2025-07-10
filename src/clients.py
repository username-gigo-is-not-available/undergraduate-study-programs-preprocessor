from minio import Minio

from src.configurations import StorageConfiguration


class MinioClient:
    MINIO_ENDPOINT_URL: str = StorageConfiguration.MINIO_ENDPOINT_URL
    MINIO_ACCESS_KEY: str = StorageConfiguration.MINIO_ACCESS_KEY
    MINIO_SECRET_KEY: str = StorageConfiguration.MINIO_SECRET_KEY
    _instance: 'MinioClient' = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.client = Minio(
                endpoint=cls.MINIO_ENDPOINT_URL,
                access_key=cls.MINIO_ACCESS_KEY,
                secret_key=cls.MINIO_SECRET_KEY,
                secure=False,
            )
        return cls._instance

    @staticmethod
    def connect():
        return MinioClient().client
