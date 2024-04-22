import pytest

from src.utils import setup_metadata_cols


@pytest.mark.usefixtures("spark_session")
def test_setup_metadata_cols(full_df_fixture):
    new_df = setup_metadata_cols(
        df=full_df_fixture, primary_key_cols=["color", "owner", "price"]
    )

    assert new_df.count() == 4
    assert new_df.columns == [
        "color",
        "owner",
        "price",
        "hash",
        "created_at",
    ]
