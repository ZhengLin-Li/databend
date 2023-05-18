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

use common_base::base::GlobalInstance;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_storages_fuse::FuseTable;

#[async_trait::async_trait]
pub trait VirtualColumnsHandler: Sync + Send {
    async fn do_generate_virtual_columns(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
    ) -> Result<()>;
}

pub struct VirtualColumnsHandlerWrapper {
    handler: Box<dyn VirtualColumnsHandler>,
}

impl VirtualColumnsHandlerWrapper {
    pub fn new(handler: Box<dyn VirtualColumnsHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_generate_virtual_columns(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
    ) -> Result<()> {
        self.handler
            .do_generate_virtual_columns(fuse_table, ctx)
            .await
    }
}

pub fn get_virtual_columns_handler() -> Arc<VirtualColumnsHandlerWrapper> {
    GlobalInstance::get()
}
