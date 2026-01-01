# DQX Check Functions Reference

The generated rules use DQX's built-in check functions. See [DQX Quality Checks Reference](https://databrickslabs.github.io/dqx/docs/reference/quality_checks/) for full details.

## Row-Level Checks

| Function | Description |
|----------|-------------|
| `is_not_null` | Value is not null |
| `is_not_empty` | Value is not empty string |
| `is_not_null_and_not_empty` | Not null and not empty string |
| `is_in_list` | Value is in allowed list |
| `is_not_in_list` | Value is not in forbidden list |
| `is_not_null_and_is_in_list` | Not null AND in allowed values |
| `is_not_null_and_not_empty_array` | Array is non-null and non-empty |
| `is_in_range` | Value within min/max boundaries |
| `is_not_in_range` | Value outside min/max boundaries |
| `is_equal_to` | Value matches specific value |
| `is_not_equal_to` | Value differs from specific value |
| `is_not_less_than` | Value meets minimum threshold |
| `is_not_greater_than` | Value respects maximum threshold |
| `is_valid_date` | Valid date format |
| `is_valid_timestamp` | Valid timestamp format |
| `is_valid_json` | Valid JSON string |
| `has_json_keys` | JSON has required keys |
| `has_valid_json_schema` | JSON conforms to schema |
| `is_not_in_future` | Timestamp not in future |
| `is_not_in_near_future` | Timestamp within acceptable future window |
| `is_older_than_n_days` | Date precedes reference by N days |
| `is_older_than_col2_for_n_days` | Column1 older than column2 by N days |
| `regex_match` | Value matches regex pattern |
| `is_valid_ipv4_address` | Valid IPv4 format |
| `is_ipv4_address_in_cidr` | IPv4 within CIDR block |
| `is_valid_ipv6_address` | Valid IPv6 format |
| `is_ipv6_address_in_cidr` | IPv6 within CIDR block |
| `sql_expression` | Custom SQL-based condition |
| `is_data_fresh` | Data not stale beyond max age |
| `does_not_contain_pii` | No personally identifiable info |
| `is_latitude` | Value between -90 and 90 |
| `is_longitude` | Value between -180 and 180 |

### Geometry Checks

| Function | Description |
|----------|-------------|
| `is_geometry` | Valid geometry value |
| `is_geography` | Valid geography value |
| `is_point` | Geometry type is point |
| `is_linestring` | Geometry type is linestring |
| `is_polygon` | Geometry type is polygon |
| `is_multipoint` | Geometry type is multipoint |
| `is_multilinestring` | Geometry type is multilinestring |
| `is_multipolygon` | Geometry type is multipolygon |
| `is_geometrycollection` | Geometry type is collection |
| `is_ogc_valid` | Geometry valid per OGC standard |
| `is_non_empty_geometry` | Geometry contains coordinates |
| `is_not_null_island` | Not at null island (0,0) |
| `has_dimension` | Geometry has specified dimension |
| `has_x_coordinate_between` | X coordinates within range |
| `has_y_coordinate_between` | Y coordinates within range |
| `is_area_not_less_than` | Geometry area meets minimum |
| `is_area_not_greater_than` | Geometry area respects maximum |
| `is_num_points_not_less_than` | Coordinate count meets minimum |
| `is_num_points_not_greater_than` | Coordinate count respects maximum |

## Dataset-Level Checks

| Function | Description |
|----------|-------------|
| `is_unique` | Values/composite keys have no duplicates |
| `is_aggr_not_greater_than` | Aggregated value respects maximum |
| `is_aggr_not_less_than` | Aggregated value meets minimum |
| `is_aggr_equal` | Aggregated value matches target |
| `is_aggr_not_equal` | Aggregated value differs from target |
| `foreign_key` | Values exist in reference dataset |
| `sql_query` | Custom SQL validates condition |
| `compare_datasets` | Compare source against reference |
| `is_data_fresh_per_time_window` | Records arrive in time windows |
| `has_valid_schema` | DataFrame schema matches expected |
| `has_no_outliers` | Detects statistical outliers |

## Example Rule

```json
{
  "check": "is_not_null",
  "column": "customer_id",
  "name": "customer_id_not_null",
  "criticality": "error"
}
```

## External Resources

- [DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Quality Checks Reference](https://databrickslabs.github.io/dqx/docs/reference/quality_checks/)
