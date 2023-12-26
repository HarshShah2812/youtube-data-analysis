import awswrangler as wr
import pandas as pd
import urllib.parse
import os

class DataProcessor:
    """Class to process data from S3 and write to Parquet files."""
    def __init__(self):
        """Initialize the DataProcessor with environment variables."""
        self.os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
        self.os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
        self.os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
        self.os_input_write_data_operation = os.environ['write_data_operation']
 
    def process_data(self, event):
        """
        Process data from an S3 event.

        Parameters
        ----------
        - event (dict): Event received from S3 trigger.

        Returns
        -------
        - wr_response: AWS Wrangler response object after writing to S3.
        """
        # Get the object from the event and show its content type
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        try:
            # Creating DF from content
            df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))
            # Extract required columns:
            df_step_1 = pd.json_normalize(df_raw['items'])
            # Write to S3
            wr_response = self.__write_to_s3(df_step_1)
            return wr_response
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
    
    def __write_to_s3(self, data):
        """
        Write DataFrame data to S3 in Parquet format.

        Parameters
        ----------
        - data (pandas.DataFrame): DataFrame to be written to S3.

        Returns
        -------
        - s3_write: AWS Wrangler response object after writing to S3.
        """
        s3_write = wr.s3.to_parquet(
                df=data,
                path=self.os_input_s3_cleansed_layer,
                dataset=True,
                database=self.os_input_glue_catalog_db_name,
                table=self.os_input_glue_catalog_table_name,
                mode=self.os_input_write_data_operation
                )
        return s3_write

def lambda_handler(event, context):
    """
    Lambda handler function.

    Parameters
    ----------
    - event (dict): Event received by the Lambda function.
    - context (object): Lambda context object.

    Returns
    -------
    - processed: Result of processing the event.
    """
    processor = DataProcessor()
    processed_data = processor.process_data(event)
    return processed_data

if __name__ == "__main__":
    sample_event = {
        'Records': [
            {
                's3': {
                    'bucket': {'name': 'youtube-raw-data-useast1-dev'},
                    'object': {'key': '*.json'}
                }
            }
        ]
    }
    result = lambda_handler(sample_event, None)
    print(result)