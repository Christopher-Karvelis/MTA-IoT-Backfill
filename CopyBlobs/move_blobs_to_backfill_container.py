from CopyBlobs.blob_storage_copier import BlobStorageCopier


def move_blobs(inputParameters: dict) -> str:
    blob_storage_reader = BlobStorageCopier()
    return blob_storage_reader.copy_blobs(**inputParameters)