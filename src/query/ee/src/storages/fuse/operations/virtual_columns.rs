// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_catalog::plan::Projection;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::VariantType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::ScalarRef;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use common_storages_fuse::io::serialize_block;
use common_storages_fuse::io::write_data;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::io::ReadSettings;
use common_storages_fuse::io::WriteSettings;
use common_storages_fuse::FuseTable;
use jsonb::array_length;
use jsonb::as_str;
use jsonb::get_by_index;
use jsonb::get_by_name;
use jsonb::object_keys;
use opendal::Operator;
use storages_common_cache::LoadParams;

#[async_backtrace::framed]
pub async fn do_generate_virtual_columns(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
) -> Result<()> {
    let snapshot_opt = fuse_table.read_table_snapshot().await?;
    let snapshot = if let Some(val) = snapshot_opt {
        val
    } else {
        // no snapshot
        return Ok(());
    };

    let table_schema = &fuse_table.get_table_info().meta.schema;

    let field_indices = table_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.data_type().remove_nullable() == TableDataType::Variant)
        .map(|(i, _)| i)
        .collect::<Vec<_>>();

    if field_indices.is_empty() {
        // no json fields
        return Ok(());
    }
    let source_schema = table_schema.project(&field_indices);

    let projection = Projection::Columns(field_indices);
    let block_reader = fuse_table.create_block_reader(projection, false, ctx.clone())?;

    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    let settings = ReadSettings::default();
    let write_settings = fuse_table.get_write_settings();
    let storage_format = write_settings.storage_format;

    let operator = fuse_table.get_operator_ref();

    for (location, ver) in &snapshot.segments {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;

        let block_metas = segment_info.block_metas()?;
        for block_meta in block_metas {
            let block = block_reader
                .read_by_meta(&settings, &block_meta, &storage_format)
                .await?;
            let virtual_location = block_meta
                .location
                .0
                .replace(".parquet", "_virtual.parquet");

            materialize_virtual_columns(
                operator,
                &write_settings,
                &virtual_location,
                &source_schema,
                block,
            )
            .await?;
        }
    }

    Ok(())
}

#[async_backtrace::framed]
async fn materialize_virtual_columns(
    operator: &Operator,
    write_settings: &WriteSettings,
    location: &str,
    source_schema: &TableSchema,
    block: DataBlock,
) -> Result<()> {
    let mut keys = Vec::new();
    for (i, _field) in source_schema.fields().iter().enumerate() {
        let block_entry = block.get_by_offset(i);
        let column = block_entry
            .value
            .convert_to_full_column(&block_entry.data_type, block.num_rows());

        let mut key_paths = BTreeMap::new();
        for row in 0..block.num_rows() {
            let val = unsafe { column.index_unchecked(row) };
            if let ScalarRef::Variant(v) = val {
                // TODO: check nestd path.
                if let Some(keys) = object_keys(v) {
                    let len = array_length(&keys).unwrap();
                    for i in 0..len {
                        let key = get_by_index(&keys, i as i32).unwrap();
                        let key = as_str(&key).unwrap();
                        let key_str = key.to_string();
                        if let Some(cnt) = key_paths.get_mut(&key_str) {
                            *cnt += 1;
                        } else {
                            key_paths.insert(key_str, 1);
                        }
                    }
                }
            }
        }
        for (key, cnt) in key_paths.iter() {
            if *cnt == block.num_rows() {
                keys.push((i, key.clone()));
            }
        }
    }
    if keys.is_empty() {
        return Ok(());
    }
    let len = block.num_rows();
    let mut virtual_fields = Vec::with_capacity(keys.len());
    let mut virtual_columns = Vec::with_capacity(keys.len());
    for (i, key) in keys {
        let source_field = source_schema.field(i);
        let virtual_name = format!("{}.{}", source_field.name(), key);
        let virtual_field = TableField::new(
            virtual_name.as_str(),
            TableDataType::Nullable(Box::new(TableDataType::Variant)),
        );
        virtual_fields.push(virtual_field);

        let block_entry = block.get_by_offset(i);
        let column = block_entry
            .value
            .convert_to_full_column(&block_entry.data_type, len);

        let mut validity = MutableBitmap::with_capacity(len);
        let mut builder = StringColumnBuilder::with_capacity(len, len * 10);
        for row in 0..len {
            let val = unsafe { column.index_unchecked(row) };
            if let ScalarRef::Variant(v) = val {
                if let Some(inner_val) = get_by_name(v, key.as_str()) {
                    validity.push(true);
                    builder.put_slice(inner_val.as_slice());
                    builder.commit_row();
                    continue;
                }
            }
            validity.push(false);
            builder.commit_row();
        }
        let column = Column::Nullable(Box::new(
            NullableColumn::<VariantType> {
                column: builder.build(),
                validity: validity.into(),
            }
            .upcast(),
        ));
        let virtual_column = BlockEntry {
            data_type: DataType::Nullable(Box::new(DataType::Variant)),
            value: Value::Column(column),
        };
        virtual_columns.push(virtual_column);
    }
    let virtual_schema = TableSchemaRefExt::create(virtual_fields);
    let virtual_block = DataBlock::new(virtual_columns, len);

    let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
    let _ = serialize_block(write_settings, &virtual_schema, virtual_block, &mut buffer)?;

    write_data(buffer, operator, location).await?;

    Ok(())
}
