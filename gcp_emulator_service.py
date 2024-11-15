import logging
import os
from google.cloud import storage
from gcp_storage_emulator.server import create_server
import constants
import json


class GCPEmulator:
    db: None = None
    logging.basicConfig(level=logging.DEBUG)

    def __init__(self):

        # default_bucket parameter creates the bucket automatically
        server = create_server(constants.GCP_HOST, constants.GCP_PORT, in_memory=False,
                               default_bucket=constants.GCP_BUCKET)
        server.start()

        os.environ["STORAGE_EMULATOR_HOST"] = f"http://{constants.GCP_HOST}:{constants.GCP_PORT}"
        client = storage.Client()
        self.bucket = client.bucket(constants.GCP_BUCKET)

    def put_data(self, key: str, value: str) -> json:
        """

        Args:
            key: Unique identifier for the data for insertion
            value: Application data (images, videos etc.)

        Returns: JSON output regarding the status of the operation

        """
        data_blob = self.bucket.blob(key)
        data_blob.upload_from_string(value)

        logging.debug("[GCP_emulator] Uploaded data for key {0}".format(key))
        return {"status": "data inserted", "data_store": constants.BLOBSTORE}

    def get_data(self, key: str) -> json:
        """

        Args:
            key: Unique identifier of the data to be retrieved

        Returns: JSON including the data/appropriate error

        """
        try:
            data_blob = self.bucket.blob(key)
            result = data_blob.download_as_string()

            logging.debug("[GCP_emulator] Downloaded data {0}".format(len(result)))
        except Exception as e:
            logging.info("[GCP_emulator] Key {0} is not in the Database".format(key))
            return {"status": "key not found", "data_store": constants.BLOBSTORE}

        return {key: result.decode('utf-8'), "data_store": constants.BLOBSTORE}
