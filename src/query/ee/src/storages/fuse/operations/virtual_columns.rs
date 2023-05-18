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

use std::sync::Arc;

use common_catalog::plan::Projection;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::TableDataType;
// use common_storages_fuse::io::MetaReaders;
// use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::FuseTable;
// use storages_common_cache::LoadParams;
// use storages_common_table_meta::meta::CompactSegmentInfo;
// use tracing::info;

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

    let field_indices = fuse_table
        .get_table_info()
        .meta
        .schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.data_type().remove_nullable() == TableDataType::Variant)
        .map(|(i, _)| i)
        .collect::<Vec<_>>();

    if field_indices.is_empty() {
        // no json columns
        return Ok(());
    }

    let projection = Projection::Columns(field_indices);
    let block_reader = fuse_table.create_block_reader(projection, false, ctx.clone())?;

    todo!()
}
