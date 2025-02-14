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

use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_management::*;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_embedded::MetaEmbedded;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_add_udf() -> Result<()> {
    let (kv_api, udf_api) = new_udf_api().await?;

    // lambda udf
    let udf = create_test_lambda_udf();
    udf_api
        .add_udf(udf.clone(), &CreateOption::CreateIfNotExists(false))
        .await?;
    let value = kv_api
        .get_kv(format!("__fd_udfs/admin/{}", udf.name).as_str())
        .await?;

    match value {
        Some(SeqV {
            seq: 1,
            meta: _,
            data: value,
        }) => {
            assert_eq!(
                value,
                serialize_struct(&udf, ErrorCode::IllegalUDFFormat, || "")?
            );
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }
    // udf server
    let udf = create_test_udf_server();
    udf_api
        .add_udf(udf.clone(), &CreateOption::CreateIfNotExists(false))
        .await?;
    let value = kv_api
        .get_kv(format!("__fd_udfs/admin/{}", udf.name).as_str())
        .await?;

    match value {
        Some(SeqV {
            seq: 2,
            meta: _,
            data: value,
        }) => {
            assert_eq!(
                value,
                serialize_struct(&udf, ErrorCode::IllegalUDFFormat, || "")?
            );
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_already_exists_add_udf() -> Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    // lambda udf
    let udf = create_test_lambda_udf();
    udf_api
        .add_udf(udf.clone(), &CreateOption::CreateIfNotExists(false))
        .await?;
    match udf_api
        .add_udf(udf.clone(), &CreateOption::CreateIfNotExists(false))
        .await
    {
        Ok(_) => panic!("Already exists add udf must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2603),
    }

    // udf server
    let udf = create_test_udf_server();
    udf_api
        .add_udf(udf.clone(), &CreateOption::CreateIfNotExists(false))
        .await?;
    match udf_api
        .add_udf(udf.clone(), &CreateOption::CreateIfNotExists(false))
        .await
    {
        Ok(_) => panic!("Already exists add udf must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2603),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_get_udfs() -> Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    let udfs = udf_api.get_udfs().await?;
    assert_eq!(udfs, vec![]);

    let lambda_udf = create_test_lambda_udf();
    let udf_server = create_test_udf_server();

    udf_api
        .add_udf(lambda_udf.clone(), &CreateOption::CreateIfNotExists(false))
        .await?;
    udf_api
        .add_udf(udf_server.clone(), &CreateOption::CreateIfNotExists(false))
        .await?;

    let udfs = udf_api.get_udfs().await?;
    assert_eq!(udfs, vec![lambda_udf, udf_server]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_drop_udf() -> Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    let lambda_udf = create_test_lambda_udf();
    let udf_server = create_test_udf_server();

    udf_api
        .add_udf(lambda_udf.clone(), &CreateOption::CreateIfNotExists(false))
        .await?;
    udf_api
        .add_udf(udf_server.clone(), &CreateOption::CreateIfNotExists(false))
        .await?;

    let udfs = udf_api.get_udfs().await?;
    assert_eq!(udfs, vec![lambda_udf.clone(), udf_server.clone()]);

    udf_api.drop_udf(&lambda_udf.name, MatchSeq::GE(1)).await?;
    udf_api.drop_udf(&udf_server.name, MatchSeq::GE(1)).await?;

    let udfs = udf_api.get_udfs().await?;
    assert_eq!(udfs, vec![]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unknown_udf_drop_udf() -> Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    let res = udf_api.drop_udf("UNKNOWN_NAME", MatchSeq::GE(1)).await;
    assert_eq!(Ok(None), res);

    Ok(())
}

fn create_test_lambda_udf() -> UserDefinedFunction {
    UserDefinedFunction::create_lambda_udf(
        "isnotempty",
        vec!["p".to_string()],
        "not(is_null(p))",
        "This is a description",
    )
}

fn create_test_udf_server() -> UserDefinedFunction {
    UserDefinedFunction::create_udf_server(
        "strlen",
        "http://localhost:8888",
        "strlen_py",
        "python",
        vec![DataType::String],
        DataType::Number(NumberDataType::Int64),
        "This is a description",
    )
}

async fn new_udf_api() -> Result<(Arc<MetaEmbedded>, UdfMgr)> {
    let test_api = Arc::new(MetaEmbedded::new_temp().await?);
    let mgr = UdfMgr::create(test_api.clone(), "admin")?;
    Ok((test_api, mgr))
}
