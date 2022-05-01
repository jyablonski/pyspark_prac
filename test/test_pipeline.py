import pytest
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# take a data frame, filter by color == 'red', group by owner, create a new col called 'grouped_price' which aggregates that.
# then make a new case when where you grab owner + grouped price and filter it based on > 10 so it's a boolean 1 or 0 and name it indicator
# and then filter the df to only values where indicator == 1.

#####
def sample_transform_jacob(input_df: DataFrame) -> DataFrame:
    inter_df = input_df.where(input_df['color'] == F.lit('red')) \
        .groupBy('owner') \
        .agg(F.sum('price') \
        .alias('grouped_price')
    )

    output_df = inter_df.select('owner', 'grouped_price', \
                                F.when(F.col('grouped_price') > 10, 1).otherwise(0).alias('indicator')) \
                                .where(F.col('indicator') == F.lit(1)
                                )
    return output_df

@pytest.mark.usefixtures("spark_session")
def test_sample_transform_jacob(spark_session):
    test_df = spark_session.createDataFrame(
        data = [
            ('red', 'Jacob', 5),
            ('red', 'Jacob', 50),
            ('green', 'Jacob2', 20),
            ('black', 'Jacob3', 1000)
        ],
        schema = ['color', 'owner', 'price']
    )
    new_df = sample_transform_jacob(test_df)
    assert new_df.count() == 1
    assert new_df.toPandas().to_dict('list')['grouped_price'][0] == 55