use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};

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

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}
