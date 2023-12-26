import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

class GlueETLJob:
    """
    Represents an AWS Glue ETL job.
    """

    def __init__(self, job_name):
        """
        Initializes the GlueETLJob instance.

        Parameters
        ----------
        - job_name (str): The name of the Glue job.
        """
        self.job_name = job_name
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(self.job_name)

    def __read_from_catalog(self, database, table, predicate_pushdown):
        """
        Reads data from the Glue Data Catalog.

        Parameters
        ----------
        - database (str): The name of the database in the Glue Data Catalog.
        - table (str): The name of the table in the Glue Data Catalog.
        - predicate_pushdown (str): Predicate pushdown for filtering.

        Returns
        -------
        - datasource0: A DynamicFrame containing the read data.
        """
        datasource0 = self.glue_context.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table,
            transformation_ctx="datasource0",
            push_down_predicate=predicate_pushdown
        )
        return datasource0

    def __apply_mapping(self, dynamic_frame):
        """
        Applies mapping to the input DynamicFrame.

        Parameters
        ----------
        - dynamic_frame (DynamicFrame): Input DynamicFrame to be mapped.

        Returns
        -------
        - mapped_dyf: A DynamicFrame with applied mapping.
        """
        mappings = [
            ("video_id", "string", "video_id", "string"),
            ("trending_date", "string", "trending_date", "string"),
            ("title", "string", "title", "string"),
            ("channel_title", "string", "channel_title", "string"),
            ("category_id", "long", "category_id", "bigint"),
            ("publish_time", "string", "publish_time", "string"),
            ("tags", "string", "tags", "string"),
            ("views", "long", "views", "bigint"),
            ("likes", "long", "likes", "bigint"),
            ("dislikes", "long", "dislikes", "bigint"),
            ("comment_count", "long", "comment_count", "bigint"),
            ("thumbnail_link", "string", "thumbnail_link", "string"),
            ("comments_disabled", "boolean", "comments_disabled", "boolean"),
            ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
            ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
            ("description", "string", "description", "string"),
            ("region", "string", "region", "string"),
            ]
        mapped_dyf = ApplyMapping.apply(frame=dynamic_frame, mappings=mappings, transformation_ctx="applymapping1")
        return mapped_dyf

    def __write_to_s3_as_parquet(self, dynamic_frame):
        """
        Writes the DynamicFrame to Amazon S3 in Parquet format.

        Parameters
        ----------
        - dynamic_frame (DynamicFrame): DynamicFrame to be written.

        Returns
        -------
        - datasink4: DynamicFrame written in parquet format
        """
        datasink1 = dynamic_frame.toDF().coalesce(1)
        df_final_output = DynamicFrame.fromDF(datasink1, self.glue_context, "df_final_output")

        datasink4 = self.glue_context.write_dynamic_frame.from_options(
            frame=df_final_output,
            connection_type="s3",
            format="parquet",
            connection_options={
                "path": "s3://youtube-cleansed-data-useast1-dev/youtube/raw_statistics/",
                "partitionKeys": ["region"]
            },
            format_options={"compression": "snappy"},
            transformation_ctx="datasink4"
        )
        return datasink4
    
    def run_job(self):
        """
        Executes the ETL job.
        """
        predicate_pushdown = "region in ('ca','gb','us')"
        datasource = self.__read_from_catalog("youtube_raw", "raw_statistics", predicate_pushdown)
        mapped_frame = self.__apply_mapping(datasource)
        self.__write_to_s3_as_parquet(mapped_frame)
        self.job.commit()

# Usage:
if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job_name = args["JOB_NAME"]

    etl_job = GlueETLJob(job_name)
    etl_job.run_job()

