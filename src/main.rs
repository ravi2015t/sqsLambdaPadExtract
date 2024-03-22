use aws_config::{BehaviorVersion, Region};
use aws_lambda_events::event::sqs::SqsEvent;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_sqs::Client;
use connectorx::prelude::*;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use polars::prelude::ParquetCompression;
use polars::prelude::*;
use polars::prelude::{DataFrame, ParquetWriter};
use std::convert::TryFrom;
use std::path::Path;
use std::sync::Arc;
use tokio::time::Instant;

#[derive(Debug)]
struct SQSMessage {
    body: String,
}

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

    let source_conn = SourceConn::try_from(
        "postgresql://dbo:dbo@partaccountpoc-cluster.cluster-ctcsve9jprsn.us-east-1.rds.amazonaws.com:5432/partaccountpoc?cxprotocol=binary",
    )
    .expect("parse conn str failed");

    let src_conn = Arc::new(source_conn);

    //check batched reads.
    let bek_ids_str = bek_ids
        .iter()
        .map(|&id| id.to_string())
        .collect::<Vec<String>>()
        .join(", ");

    let query = format!(
        "SELECT * FROM part_account WHERE part_account.bekId IN ({})",
        bek_ids_str
    );
    let df_handle = tokio::task::spawn_blocking(move || {
        let queries = &[CXQuery::from(query.as_str())];

        // let mut destination: ArrowDestination =
        //     get_arrow(&src_conn, None, queries).expect("run failed");

        // let df = destination.arrow().unwrap();

        let destination: Arrow2Destination =
            get_arrow2(&src_conn, None, queries).expect("run failed");
        let df: DataFrame = destination.polars().unwrap();

        // tracing::info!("Df schema {:?}", df.schema());
        df
    })
    .await;

    let df = df_handle.unwrap();

    let mut handles = vec![];

    // Iterate over bek_ids concurrently
    for bek_id in bek_ids {
        let df_clone = df.clone();
        // Spawn a task for each bek_id and store the handle
        let handle = tokio::spawn(async move {
            filter_and_write_parquet(df_clone, bek_id).await;
            transfer_to_s3(bek_id.into()).await;
            write_to_sqs(bek_id.into()).await;
        });
        handles.push(handle);
    }

    // Await on all handles to ensure all tasks have completed
    for handle in handles {
        handle.await.unwrap();
    }

    let end = Instant::now();
    tracing::debug!("Transform was completed in time {:?} ", end - start);

    Ok(())
}

async fn filter_and_write_parquet(df: DataFrame, bek_id: u16) {
    let mut filtered_df = df
        .lazy()
        .filter(col("bekid").eq(lit(bek_id)))
        .collect()
        .unwrap();

    ParquetWriter::new(std::fs::File::create(format!("/tmp/result{}.parquet", bek_id)).unwrap())
        .with_statistics(true)
        .set_parallel(true)
        .with_compression(ParquetCompression::Uncompressed)
        .finish(&mut filtered_df)
        .unwrap();
}

async fn transfer_to_s3(id: i32) {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&config);
    let bucket_name = "pensioncalcseast1";

    let filename = format!("/tmp/result{}.parquet", id);
    let body = ByteStream::from_path(Path::new(&filename)).await;

    let s3_key = format!("part_account_from_rds/{}/pa_detail.parquet", id);

    let response = s3_client
        .put_object()
        .bucket(bucket_name)
        .body(body.unwrap())
        .key(&s3_key)
        .send()
        .await;

    match response {
        Ok(_) => {
            tracing::debug!(
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

async fn write_to_sqs(id: i32) {
    let region = Region::new("us-east-1");
    let shared_config = aws_config::from_env().region(region).load().await;
    let client = Client::new(&shared_config);
    let queue_url = "".to_string();
    let message = SQSMessage {
        body: format!("bekId:{}", id),
    };

    let res = send(&client, &queue_url, &message).await;
    tracing::info!("Repsonse after send {:?}", res.unwrap());
}

async fn send(client: &Client, queue_url: &String, message: &SQSMessage) -> Result<(), Error> {
    let rsp = client
        .send_message()
        .queue_url(queue_url)
        .message_body(&message.body)
        .send()
        .await?;
    tracing::debug!("Response from queue {:?}", rsp);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}
