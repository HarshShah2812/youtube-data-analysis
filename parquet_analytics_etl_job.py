import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

class GlueETLJob:
    """
    Represents an AWS Glue ETL job.
    """
    
    def __init__(self, job_name):
        """
        Initialises the GlueETLJob instance.
        
        Args: 
        - job_name(str): The name of the Glue job
        """
        self.job_name = job_name
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.job = Job(self.glue_context)
        self.job.init(self.job_name)

    def __read_from_catalog(self, database, table, transformation_ctx):
        """
        Reads data from the Glue Data Catalog.

        Args:
        - database (str): The name of the database in the Glue Data Catalog.
        - table (str): The name of the table in the Glue Data Catalog.
        - transformation_ctx (str): Transformation context for tracking.

        Returns:
        - dyf: A DynamicFrame containing the read data.
        """
        # Creating a DynamicFrame using a specified catalog namespace and table name
        dyf = self.glue_context.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table,
            transformation_ctx=transformation_ctx,
        )
        return dyf

    def __join_frames(self, frame1, frame2, keys1, keys2, transformation_ctx):
        """
        Joins two DynamicFrames.

        Args:
        - frame1 (DynamicFrame): The first DynamicFrame to be joined.
        - frame2 (DynamicFrame): The second DynamicFrame to be joined.
        - keys1 (list): List of keys from frame1 for the join.
        - keys2 (list): List of keys from frame2 for the join.
        - transformation_ctx (str): Transformation context for tracking.

        Returns:
        - frame_join: A DynamicFrame resulting from the join operation.
        """
        frame_join = Join.apply(
            frame1=frame1,
            frame2=frame2,
            keys1=keys1,
            keys2=keys2,
            transformation_ctx=transformation_ctx,
        )
        return frame_join

    def __write_to_s3(self, sink_path, connection_type, update_behavior, partition_keys,
                    compression, enable_update_catalog, transformation_ctx, catalog_db, catalog_table,
                    sink_format, frame):
        """
        Writes a DynamicFrame to Amazon S3.

        Args:
        - sink_path (str): The S3 path to write the data.
        - connection_type (str): The connection type for writing to S3.
        - update_behavior (str): The update behavior for existing data.
        - partition_keys (list): List of partition keys for S3 partitioning.
        - compression (str): Compression type for data.
        - enable_update_catalog (bool): Enable catalog update.
        - transformation_ctx (str): Transformation context for tracking.
        - catalog_db (str): The name of the catalog database.
        - catalog_table (str): The name of the catalog table.
        - sink_format (str): The format of data to be written.
        - frame (DynamicFrame): The DynamicFrame to be written.

        Returns:
        - None
        """
        # Get the sink for writing data to S3
        sink = self.glue_context.getSink(
            path=sink_path,
            connection_type=connection_type,
            updateBehavior=update_behavior,
            partitionKeys=partition_keys,
            compression=compression,
            enableUpdateCatalog=enable_update_catalog,
            transformation_ctx=transformation_ctx,
        )
        # Set catalog info and format for writing
        sink.setCatalogInfo(catalogDatabase=catalog_db, catalogTableName=catalog_table)
        sink.setFormat(sink_format)
        # Write the DynamicFrame to S3
        sink.writeFrame(frame)

    def run_job(self):
        """
        Executes the ETL job.
        """
        # Read data from Glue Data Catalog
        frame1 = self.__read_from_catalog("db_youtube_cleaned", "raw_statistics", "AWSGlueDataCatalog_node1")
        frame2 = self.__read_from_catalog("db_youtube_cleaned", "cleaned_statistics_reference_data", "AWSGlueDataCatalog_node2")

        # Join the obtained frames
        joined_frame = self.__join_frames(
            frame1=frame1,
            frame2=frame2,
            keys1=["category_id"],
            keys2=["id"],
            transformation_ctx="Join_node1703674113541",
        )

        # Write the joined frame to Amazon S3
        self.__write_to_s3(
            sink_path="s3://youtube-analytics-data-useast1-dev",
            connection_type="s3",
            update_behavior="UPDATE_IN_DATABASE",
            partition_keys=["region", "category_id"],
            compression="snappy",
            enable_update_catalog=True,
            transformation_ctx="AmazonS3_node1703674446828",
            catalog_db="db_youtube_analytics",
            catalog_table="final_analytics",
            sink_format="glueparquet",
            frame=joined_frame
        )

        # Commit the Glue job
        self.job.commit()

# Usage:
if __name__ == "__main__":
    # Assuming the job name is provided as an argument
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job_name = args["JOB_NAME"]

    # Create an instance of GlueETLJob and execute the job
    etl_job = GlueETLJob(job_name)
    etl_job.run_job()