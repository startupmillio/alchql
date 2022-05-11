from alchql.gql_fields import camel_to_snake


def test_camel_to_snake():
    assert camel_to_snake("artistId_Eq") == "artist_id__eq"
    assert camel_to_snake("sort") == "sort"
    assert camel_to_snake("priceMomentum12mo") == "price_momentum_12mo"
    assert camel_to_snake("s3PresignedUrl") == "s3_presigned_url"
    assert camel_to_snake("S3PresignedUrl") == "s3_presigned_url"
    assert camel_to_snake("thumbnailS3PresignedUrl") == "thumbnail_s3_presigned_url"
