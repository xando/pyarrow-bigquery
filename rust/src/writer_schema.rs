//! Convert Python writer types to the Arrow representations accepted by BigQuery.
//!
//! Schema validation happens before table creation. Value conversions are exact:
//! nanoseconds must be whole microseconds and unsigned values must fit INT64.

use std::sync::Arc;

use arrow::array::{
    make_array, Array, ArrayRef, Date32Array, Date64Array, Decimal128Array, GenericListArray,
    Int64Array, ListArray, MutableArrayData, OffsetSizeTrait, RecordBatch, StructArray,
    TimestampMicrosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;

fn invalid(message: impl Into<String>) -> ArrowError {
    ArrowError::InvalidArgumentError(message.into())
}

pub(crate) fn normalize_schema(schema: &Schema) -> Result<Schema, ArrowError> {
    if schema.fields().is_empty() {
        return Err(invalid("BigQuery write requires a non-empty schema"));
    }
    let fields = schema
        .fields()
        .iter()
        .map(|field| normalize_field(field))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn normalize_field(field: &Field) -> Result<Field, ArrowError> {
    let data_type = normalize_type(field.data_type())
        .map_err(|error| invalid(format!("field '{}': {error}", field.name())))?;
    Ok(field.clone().with_data_type(data_type))
}

fn normalize_type(data_type: &DataType) -> Result<DataType, ArrowError> {
    Ok(match data_type {
        DataType::Boolean | DataType::Binary | DataType::Utf8 | DataType::Date32 => {
            data_type.clone()
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DataType::Int64,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => DataType::Float64,
        DataType::LargeUtf8 => DataType::Utf8,
        DataType::Date64 => DataType::Date32,
        // Python's schema mapping treats naive and timezone-aware timestamps as
        // TIMESTAMP. Keep their epoch values and explicitly mark the wire UTC.
        DataType::Timestamp(_, _) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Decimal128(precision, scale)
            if *precision > 0
                && *precision <= 38
                && *scale <= 9
                && i16::from(*precision) - i16::from(*scale) <= 29 =>
        {
            DataType::Decimal128(38, 9)
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            return Err(invalid(format!(
                "{data_type} cannot be written exactly as BigQuery NUMERIC; engine='rust' requires Decimal128 with scale <= 9 and precision - scale <= 29"
            )));
        }
        DataType::List(field) | DataType::LargeList(field) => {
            if matches!(
                field.data_type(),
                DataType::List(_) | DataType::LargeList(_)
            ) {
                return Err(invalid("BigQuery does not support directly nested lists"));
            }
            DataType::List(Arc::new(normalize_field(field)?))
        }
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(invalid("BigQuery does not support empty structs"));
            }
            DataType::Struct(
                fields
                    .iter()
                    .map(|field| normalize_field(field))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )
        }
        _ => {
            return Err(invalid(format!(
                "{data_type} is unsupported by engine='rust'; use engine='python' for types supported by the Python writer"
            )));
        }
    })
}

pub(crate) fn normalize_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    if &normalize_schema(batch.schema().as_ref())? != schema.as_ref() {
        return Err(invalid("batch schema does not match the writer schema"));
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| normalize_array(array, field.data_type(), None))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema.clone(), columns)
}

fn normalize_array(
    array: &ArrayRef,
    data_type: &DataType,
    parent_validity: Option<&NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    match (array.data_type(), data_type) {
        (DataType::List(_), DataType::List(field)) => {
            normalize_list::<i32>(array, field, parent_validity)
        }
        (DataType::LargeList(_), DataType::List(field)) => {
            normalize_list::<i64>(array, field, parent_validity)
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let validity = NullBuffer::union(parent_validity, array.nulls());
            let columns = array
                .columns()
                .iter()
                .zip(fields)
                .map(|(column, field)| {
                    normalize_array(column, field.data_type(), validity.as_ref())
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                validity,
            )?))
        }
        _ => {
            // A null struct may contain arbitrary physical child values. Mask
            // those values before checked casts so they cannot cause false errors.
            let array = if parent_validity.is_some() {
                make_array(
                    array
                        .to_data()
                        .into_builder()
                        .nulls(NullBuffer::union(parent_validity, array.nulls()))
                        .build()?,
                )
            } else {
                array.clone()
            };
            normalize_scalar(&array, data_type)
        }
    }
}

fn normalize_scalar(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef, ArrowError> {
    let strict = CastOptions {
        safe: false, // Arrow's safe=true replaces overflows with nulls.
        ..Default::default()
    };
    match array.data_type() {
        DataType::Timestamp(unit, _) => {
            let integers = cast_with_options(array.as_ref(), &DataType::Int64, &strict)?;
            let integers = integers.as_any().downcast_ref::<Int64Array>().unwrap();
            let values = integers
                .iter()
                .map(|value| {
                    value
                        .map(|value| match unit {
                            TimeUnit::Second => value.checked_mul(1_000_000).ok_or_else(|| {
                                invalid("timestamp overflows microsecond representation")
                            }),
                            TimeUnit::Millisecond => value.checked_mul(1_000).ok_or_else(|| {
                                invalid("timestamp overflows microsecond representation")
                            }),
                            TimeUnit::Microsecond => Ok(value),
                            TimeUnit::Nanosecond if value % 1_000 == 0 => Ok(value / 1_000),
                            TimeUnit::Nanosecond => Err(invalid(
                                "nanosecond timestamp loses precision in BigQuery; values must be whole microseconds",
                            )),
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(
                TimestampMicrosecondArray::from(values).with_timezone("UTC"),
            ))
        }
        DataType::Date64 => {
            let array = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let values = array
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            if value % 86_400_000 != 0 {
                                return Err(invalid(
                                    "Date64 values must be whole days to write BigQuery DATE without precision loss",
                                ));
                            }
                            i32::try_from(value / 86_400_000)
                                .map_err(|_| invalid("Date64 value overflows Date32 representation"))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(Date32Array::from(values)))
        }
        DataType::Decimal128(precision, _) => {
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .validate_decimal_precision(*precision)?;
            cast_with_options(array.as_ref(), data_type, &strict)
        }
        _ => cast_with_options(array.as_ref(), data_type, &strict),
    }
}

fn normalize_list<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &Arc<Field>,
    parent_validity: Option<&NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .unwrap();
    let validity = NullBuffer::union(parent_validity, array.nulls());
    // Python's protobuf serializer omits null repeated records. Do the same,
    // before descending into their possibly arbitrary physical child values.
    let omit_null_records = matches!(array.values().data_type(), DataType::Struct(_))
        && array.values().null_count() > 0;
    let mut offsets = vec![0_i32];
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut count = 0_usize;
    for (index, pair) in array.value_offsets().windows(2).enumerate() {
        let (start, end) = (pair[0].as_usize(), pair[1].as_usize());
        if validity
            .as_ref()
            .is_none_or(|validity| validity.is_valid(index))
            && start < end
        {
            if omit_null_records {
                for index in start..end {
                    if array.values().is_valid(index) {
                        keep_range(&mut ranges, &mut count, index, index + 1)?;
                    }
                }
            } else {
                keep_range(&mut ranges, &mut count, start, end)?;
            }
        }
        offsets.push(
            i32::try_from(count).map_err(|_| invalid("list values exceed Arrow List capacity"))?,
        );
    }
    // Compact first: null lists may have hidden values, and sliced arrays may
    // retain children outside their selected range. Neither should be converted.
    let values = match ranges.as_slice() {
        [] => array.values().slice(0, 0),
        [(start, end)] => array.values().slice(*start, end - start),
        _ => {
            let data = array.values().to_data();
            let mut compacted = MutableArrayData::new(vec![&data], false, count);
            for (start, end) in ranges {
                compacted.extend(0, start, end);
            }
            make_array(compacted.freeze())
        }
    };
    if values.null_count() > 0 {
        return Err(invalid(
            "BigQuery repeated scalar fields cannot contain null elements",
        ));
    }
    let values = normalize_array(&values, field.data_type(), None)?;
    Ok(Arc::new(ListArray::try_new(
        field.clone(),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    )?))
}

fn keep_range(
    ranges: &mut Vec<(usize, usize)>,
    count: &mut usize,
    start: usize,
    end: usize,
) -> Result<(), ArrowError> {
    *count = count
        .checked_add(end - start)
        .ok_or_else(|| invalid("list values length overflow"))?;
    match ranges.last_mut() {
        Some((_, previous_end)) if *previous_end == start => *previous_end = end,
        _ => ranges.push((start, end)),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int32Array, LargeStringArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
    };
    use std::collections::HashMap;

    fn batch(array: ArrayRef) -> RecordBatch {
        let field = Field::new("value", array.data_type().clone(), true)
            .with_metadata(HashMap::from([("field-key".into(), "field-value".into())]));
        let schema = Schema::new_with_metadata(
            vec![field],
            HashMap::from([("schema-key".into(), "schema-value".into())]),
        );
        RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap()
    }

    fn normalized(array: ArrayRef) -> Result<RecordBatch, ArrowError> {
        let batch = batch(array);
        let schema = Arc::new(normalize_schema(batch.schema().as_ref())?);
        normalize_batch(&batch, &schema)
    }

    #[test]
    fn preserves_metadata_and_nullability_while_canonicalizing() {
        let input = batch(Arc::new(Int32Array::from(vec![Some(7), None])));
        let schema = Arc::new(normalize_schema(input.schema().as_ref()).unwrap());
        assert_eq!(schema.metadata(), input.schema().metadata());
        assert_eq!(
            schema.field(0).metadata(),
            input.schema().field(0).metadata()
        );
        assert_eq!(
            schema.field(0).is_nullable(),
            input.schema().field(0).is_nullable()
        );
        let output = normalize_batch(&input, &schema).unwrap();
        assert_eq!(output.column(0).data_type(), &DataType::Int64);
        assert_eq!(output.column(0).null_count(), 1);
        let string =
            normalized(Arc::new(LargeStringArray::from(vec![Some("hello"), None]))).unwrap();
        assert_eq!(string.column(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn timestamps_preserve_epoch_and_convert_exactly_to_utc_microseconds() {
        for timezone in [None, Some("Europe/Warsaw")] {
            let input = TimestampSecondArray::from(vec![Some(-2), Some(1), None])
                .with_timezone_opt(timezone);
            let output = normalized(Arc::new(input)).unwrap();
            let output = output
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(
                output,
                &TimestampMicrosecondArray::from(vec![Some(-2_000_000), Some(1_000_000), None])
                    .with_timezone("UTC")
            );
        }
        let output = normalized(Arc::new(TimestampNanosecondArray::from(vec![
            -1_000, 2_000,
        ])))
        .unwrap();
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.values(), &[-1, 2]);
    }

    #[test]
    fn rejects_overflow_and_precision_loss_instead_of_truncating_or_nulling() {
        let error = normalized(Arc::new(UInt64Array::from(vec![u64::MAX]))).unwrap_err();
        assert!(error.to_string().contains("cast"));
        let error = normalized(Arc::new(TimestampSecondArray::from(vec![i64::MAX]))).unwrap_err();
        assert!(error.to_string().contains("overflows"));
        let error = normalized(Arc::new(TimestampNanosecondArray::from(vec![1]))).unwrap_err();
        assert!(error.to_string().contains("precision"));
        let error = normalized(Arc::new(Date64Array::from(vec![1]))).unwrap_err();
        assert!(error.to_string().contains("whole days"));
    }

    #[test]
    fn date_and_decimal_values_are_exact() {
        let output = normalized(Arc::new(Date64Array::from(vec![
            -86_400_000,
            0,
            86_400_000,
        ])))
        .unwrap();
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .values(),
            &[-1, 0, 1]
        );
        let decimals = Decimal128Array::from(vec![Some(-123), None, Some(456)])
            .with_precision_and_scale(5, 2)
            .unwrap();
        let output = normalized(Arc::new(decimals)).unwrap();
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.data_type(), &DataType::Decimal128(38, 9));
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(-1_230_000_000), None, Some(4_560_000_000)]
        );
        let invalid = Decimal128Array::from(vec![1000])
            .with_precision_and_scale(2, 0)
            .unwrap();
        assert!(normalized(Arc::new(invalid)).is_err());
    }

    #[test]
    fn rejects_unsupported_schemas_before_writing() {
        let nested = DataType::List(Arc::new(Field::new(
            "item",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )));
        for data_type in [
            DataType::Null,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::LargeBinary,
            DataType::Decimal128(38, 0),
            DataType::Decimal128(20, 10),
            DataType::Decimal256(40, 9),
            DataType::Struct(Vec::<Field>::new().into()),
            nested,
        ] {
            assert!(
                normalize_schema(&Schema::new(vec![Field::new("value", data_type, true)])).is_err()
            );
        }
        assert!(normalize_schema(&Schema::empty()).is_err());
    }

    fn sliced_lists<O: OffsetSizeTrait>() {
        // Overflowing hidden children must be removed before integer conversion.
        let values = Arc::new(UInt64Array::from(vec![
            u64::MAX,
            1,
            u64::MAX,
            2,
            3,
            u64::MAX,
        ]));
        let offsets = [0, 1, 2, 3, 3, 5, 6]
            .into_iter()
            .map(O::usize_as)
            .collect::<Vec<_>>();
        let list = GenericListArray::<O>::new(
            Arc::new(Field::new("item", DataType::UInt64, true)),
            OffsetBuffer::new(offsets.into()),
            values,
            Some(NullBuffer::from(vec![true, true, false, true, true, true])),
        );
        let output = normalized(Arc::new(list.slice(1, 4))).unwrap();
        let list = output
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.null_count(), 0);
        assert_eq!(list.value_offsets(), &[0, 1, 1, 1, 3]);
        assert_eq!(
            list.values().as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![1, 2, 3])
        );
    }

    #[test]
    fn normalizes_null_and_sliced_lists_and_discards_hidden_values() {
        sliced_lists::<i32>();
        sliced_lists::<i64>();
    }

    #[test]
    fn rejects_visible_null_scalar_elements_but_ignores_hidden_ones() {
        let lists = ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(Int64Array::from(vec![None, Some(2)])),
            None,
        );
        assert!(normalized(Arc::new(lists.clone()))
            .unwrap_err()
            .to_string()
            .contains("null elements"));
        let hidden = ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            lists.offsets().clone(),
            lists.values().clone(),
            Some(NullBuffer::from(vec![false, true])),
        );
        assert!(normalized(Arc::new(hidden)).is_ok());
    }

    #[test]
    fn normalizes_lists_recursively_under_null_structs() {
        let lists = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, false)),
            OffsetBuffer::new(vec![0, 1, 2, 3, 4].into()),
            Arc::new(UInt64Array::from(vec![1, u64::MAX, u64::MAX, 2])),
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        let records = StructArray::new(
            vec![
                Field::new("list", lists.data_type().clone(), true),
                Field::new("number", DataType::UInt64, false),
            ]
            .into(),
            vec![
                Arc::new(lists),
                Arc::new(UInt64Array::from(vec![10, 11, u64::MAX, 13])),
            ],
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        let output = normalized(Arc::new(records.clone())).unwrap();
        let normalized_records = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(normalized_records.is_null(2));
        let normalized_lists = normalized_records
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(normalized_lists.value_offsets(), &[0, 1, 1, 1, 2]);
        let outer = ListArray::new(
            Arc::new(Field::new("item", records.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 4].into()),
            Arc::new(records),
            None,
        );
        let output = normalized(Arc::new(outer)).unwrap();
        let outer = output
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let records = outer
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records.null_count(), 0);
        assert_eq!(
            records
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10, 11, 13]
        );
        let lists = records
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(lists.null_count(), 0);
        assert_eq!(lists.value_offsets(), &[0, 1, 1, 2]);
        assert_eq!(
            lists
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![1, 2])
        );
    }
}
