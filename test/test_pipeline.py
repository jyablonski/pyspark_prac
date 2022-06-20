import pytest
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType

full_df_schema = 'StructType(List(StructField(owner,StringType,true),StructField(price,IntegerType,true)))'

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
def test_full_df_transform(full_df_fixture):
    new_df = sample_transform_jacob(full_df_fixture)
    assert new_df.count() == 1
    assert new_df.toPandas().to_dict('list')['grouped_price'][0] == 55

# im curious if it's best practice to use spark dtypes like below, or just use python / pandas df.dtypes
@pytest.mark.usefixtures("spark_session")
def test_df_schema_col_dtype(df_dtype_fixture):
    new_df = df_dtype_fixture.withColumn("price", df_dtype_fixture.price.cast('int'))

    assert new_df.schema['price'].dataType == IntegerType()

@pytest.mark.usefixtures("spark_session")
def test_df_schema(df_dtype_fixture):
    new_df = df_dtype_fixture.withColumn("price", df_dtype_fixture.price.cast('int'))

    # adding str here allows me to avoid having to import all of this StructType Field nonsense
    # basically just grab the schema once and then if it ever changes (unexpectedly) then this test will start failing.
    assert str(new_df.schema) == full_df_schema