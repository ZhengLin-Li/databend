// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_exception::Result;
use common_expression::DataSchemaRef;
use common_license::license_manager::get_license_manager;
use common_sql::plans::GenerateVirtualColumnsPlan;
use common_storages_fuse::FuseTable;
use virtual_columns_handler::get_virtual_columns_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[allow(dead_code)]
pub struct GenerateVirtualColumnsInterpreter {
    ctx: Arc<QueryContext>,
    plan: GenerateVirtualColumnsPlan,
}

impl GenerateVirtualColumnsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: GenerateVirtualColumnsPlan) -> Result<Self> {
        Ok(GenerateVirtualColumnsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for GenerateVirtualColumnsInterpreter {
    fn name(&self) -> &str {
        "GenerateVirtualColumnsInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager.manager.check_enterprise_enabled(
            &self.ctx.get_settings(),
            self.ctx.get_tenant(),
            "generate_virtual_columns".to_string(),
        )?;

        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let tbl_name = self.plan.table.clone();
        let table = self
            .ctx
            .get_table(&catalog_name, &db_name, &tbl_name)
            .await?;
        let ctx = self.ctx.clone();

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let handler = get_virtual_columns_handler();
        let _res = handler.do_generate_virtual_columns(fuse_table, ctx).await?;

        Ok(PipelineBuildResult::create())
    }
}
