use aws_config::{BehaviorVersion, Region};
use aws_lambda_events::event::sqs::{BatchItemFailure, SqsBatchResponse, SqsEvent};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_sqs::Client;
use connectorx::prelude::*;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use polars::prelude::ParquetCompression;
use polars::prelude::*;
use polars::prelude::{DataFrame, ParquetWriter};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Debug)]
struct SQSMessage {
    body: String,
}

async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<SqsBatchResponse, Error> {
    // Extract some useful information from the request

    let mut batch_item_failures: Vec<BatchItemFailure> = vec![];

    let recs = event.payload.records;

    let (bek_ids, bek_id_map): (Vec<u16>, HashMap<u16, String>) = recs
        .iter()
        .filter_map(|sqs_message| {
            sqs_message
                .body
                .as_ref()
                .map(|body| (sqs_message.message_id.clone(), body))
        })
        .filter_map(|(message_id, body)| {
            body.split(":")
                .nth(1) // Extract the second part after splitting by ":"
                .and_then(|bek_id_str| bek_id_str.trim().parse().ok()) // Parse to u16
                .map(|bek_id: u16| (bek_id, message_id.clone()))
        })
        .fold(
            (Vec::new(), HashMap::new()),
            |(mut ids, mut map), (bek_id, message_id)| {
                ids.push(bek_id);
                map.insert(bek_id, message_id.unwrap());
                (ids, map)
            },
        );

    let start = Instant::now();

    //refactor to get the Password from Secrets Manager
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
        let destination: Arrow2Destination =
            get_arrow2(&src_conn, None, queries).expect("run failed");
        let df: DataFrame = destination.polars().unwrap();
        df
    })
    .await;

    let df = df_handle.unwrap();

    let mut handles = vec![];

    // Iterate over bek_ids concurrently
    for bek_id in bek_ids {
        let df_clone = df.clone();
        // Spawn a task for each bek_id and store the handle
        let handle: JoinHandle<Result<(), (u16, Error)>> = tokio::spawn(async move {
            if let Err(err) = filter_and_write_parquet(df_clone, bek_id).await {
                tracing::error!(err=%err, "failed to filter and write parquet");
                return Err((bek_id, err));
            }

            if let Err(err) = transfer_to_s3(bek_id.into()).await {
                tracing::error!(err=%err, "failed to upload data to S3");
                return Err((bek_id, err));
            }

            if let Err(err) = write_to_sqs(bek_id.into()).await {
                tracing::error!(err=%err, "failed to write to SQS");
                return Err((bek_id, err));
            }

            Ok(())
        });

        handles.push(handle);
    }
    // Await on all handles to ensure all tasks have completed
    for handle in handles {
        if let Err((bek_id, _)) = handle.await.unwrap() {
            // If there was an error, retrieve the corresponding message ID and add it to failed_message_ids
            if let Some(&ref message_id) = bek_id_map.get(&bek_id) {
                batch_item_failures.push(BatchItemFailure {
                    item_identifier: message_id.clone(),
                });
            }
        }
    }

    let end = Instant::now();
    tracing::debug!("Transform was completed in time {:?} ", end - start);

    Ok(SqsBatchResponse {
        batch_item_failures,
    })
}

async fn filter_and_write_parquet(df: DataFrame, bek_id: u16) -> Result<(), Error> {
    let mut filtered_df = df.lazy().filter(col("bekid").eq(lit(bek_id))).collect()?;

    ParquetWriter::new(std::fs::File::create(format!("/tmp/result{}.parquet", bek_id)).unwrap())
        .with_statistics(true)
        .set_parallel(true)
        .with_compression(ParquetCompression::Uncompressed)
        .finish(&mut filtered_df)?;
    Ok(())
}

async fn transfer_to_s3(id: i32) -> Result<(), Error> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&config);
    let bucket_name = "pensioncalcseast1";

    let filename = format!("/tmp/result{}.parquet", id);
    let body = ByteStream::from_path(Path::new(&filename)).await;

    let s3_key = format!("part_account_from_rds/{}/pa_detail.parquet", id);

    s3_client
        .put_object()
        .bucket(bucket_name)
        .body(body.unwrap())
        .key(&s3_key)
        .send()
        .await?;

    Ok(())
}

async fn write_to_sqs(id: i32) -> Result<(), Error> {
    let region = Region::new("us-east-1");
    let shared_config = aws_config::from_env().region(region).load().await;
    let client = Client::new(&shared_config);
    let queue_url = "".to_string();
    let message = SQSMessage {
        body: format!("bekId:{}", id),
    };

    send(&client, &queue_url, &message).await?;
    Ok(())
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
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();
    run(service_fn(function_handler)).await
}
