from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from p2.config.ConfigStore import *
from p2.udfs.UDFs import *
from prophecy.utils import *
from p2.graph import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/P2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/P2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
