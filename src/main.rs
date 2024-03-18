use aws_config::BehaviorVersion;
use aws_lambda_events::event::sqs::SqsEvent;
use aws_sdk_s3::primitives::ByteStream;
use connectorx::prelude::*;
use futures::future::try_join_all;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use polars::prelude::ParquetCompression;
use polars::prelude::{df, DataFrame, ParquetWriter};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{convert::TryFrom, rc::Rc};
use tokio::time::Instant;

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    // Extract some useful information from the request

    let recs = event.payload.records;
    let bek_ids: Vec<u16> = recs
        .iter()
        .filter_map(|sqs_message| sqs_message.body.as_ref()) // Filter out None values
        .filter_map(|body| {
            body.split(":")
                .nth(1) // Extract the second part after splitting by ":"
                .and_then(|bek_id_str| bek_id_str.trim().parse().ok()) // Parse to u16
        })
        .collect();
    tracing::info!("The BEK ids supplied {:?} ", bek_ids);

    let start = Instant::now();

    tracing::info!("Inside the lambda function. ");

    let source_conn = SourceConn::try_from(
        "postgresql://dbo:dbo@partaccountpoc-cluster.cluster-ctcsve9jprsn.us-east-1.rds.amazonaws.com:5432/partaccountpoc?cxprotocol=binary",
    )
    .expect("parse conn str failed");

    let src_conn = Arc::new(source_conn);

    tracing::info!("After getting back connection.");

    //check batched reads.
    let bek_ids_str = bek_ids
        .iter()
        .map(|&id| id.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    let start_id: i32 = bek_ids[0].into();

    let query = format!(
        "SELECT * FROM part_account WHERE part_account.bekId IN ({})",
        bek_ids_str
    );
    let _handle = tokio::task::spawn_blocking(move || {
        // let query = format!(
        //     "SELECT * FROM part_account where part_account.bekid>={} and part_account.bekid<={}",
        //     start_id, end_id
        // );
        let queries = &[CXQuery::from(query.as_str())];
        let destination: Arrow2Destination =
            get_arrow2(&src_conn, None, queries).expect("run failed");
        let mut df: DataFrame = destination.polars().unwrap();

        ParquetWriter::new(
            std::fs::File::create(format!("/tmp/result{}.parquet", start_id)).unwrap(),
        )
        .with_statistics(true)
        .set_parallel(true)
        .with_compression(ParquetCompression::Uncompressed)
        .finish(&mut df)
        .unwrap();

        tracing::info!("Df size {:?}", df.height());
    })
    .await;

    transfer_to_s3(start_id).await;

    let end = Instant::now();
    let message = format!("Transform was completed in time {:?} ", end - start);

    Ok(())
}

async fn transfer_to_s3(id: i32) {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&config);
    let bucket_name = "pensioncalcseast1";

    let filename = format!("/tmp/result{}.parquet", id);
    let body = ByteStream::from_path(Path::new(&filename)).await;

    let s3_key = format!("results/result{}.parquet", id);

    let response = s3_client
        .put_object()
        .bucket(bucket_name)
        .body(body.unwrap())
        .key(&s3_key)
        .send()
        .await;

    match response {
        Ok(_) => {
            tracing::info!(
                filename = %filename,
                "data successfully stored in S3",
            );
            // Return `Response` (it will be serialized to JSON automatically by the runtime)
        }
        Err(err) => {
            // In case of failure, log a detailed error to CloudWatch.
            tracing::error!(
                err = %err,
                filename = %filename,
                "failed to upload data to S3"
            );
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}
