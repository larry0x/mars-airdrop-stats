use std::{fs, path::PathBuf};

use clap::Parser;
use cosmos_sdk_proto::{cosmos, traits::MessageExt};
use futures::future;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Input {
    address: String,
    amount: u128,
}

#[derive(Serialize)]
struct Output {
    address: String,

    /// How many txs has the account sent since Mars Hub launch
    sequence: u64,

    /// How many MARS tokens the account received at launch
    airdrop_amount: u128,

    /// How many MARS tokens the account is currently staking
    staked_amount: u128,
}

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Decode(#[from] prost::DecodeError),

    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),

    #[error(transparent)]
    Status(#[from] tonic::Status),

    #[error("Account not found: {address}")]
    AccountNotFound {
        address: String,
    },
}

#[derive(Parser)]
pub struct Cli {
    /// Input JSON file containing airdrop data
    #[arg(long)]
    pub input: PathBuf,

    /// Output file containing user sequence and staked amount
    #[arg(long)]
    pub output: PathBuf,

    /// URL to a gRPC endpoint
    #[arg(long)]
    pub grpc_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    let airdrop_data = fs::read_to_string(&cli.input)?;
    let mut users: Vec<Input> = serde_json::from_str(&airdrop_data)?;
    users.truncate(5);

    let output = future::try_join_all(users.into_iter().map(|user| {
        // https://stackoverflow.com/questions/66429545/clone-a-string-for-an-async-move-closure-in-rust
        let grpc_url = cli.grpc_url.clone();
        async move {
            let sequence = cosmos::auth::v1beta1::query_client::QueryClient::connect(grpc_url.clone())
                .await?
                .account(cosmos::auth::v1beta1::QueryAccountRequest {
                    address: user.address.clone(),
                })
                .await?
                .into_inner()
                .account
                .as_ref()
                .map(<cosmos::auth::v1beta1::BaseAccount>::from_any)
                .transpose()?
                .ok_or_else(|| Error::AccountNotFound {
                    address: user.address.clone(),
                })?
                .sequence;

            let staked_amount = cosmos::staking::v1beta1::query_client::QueryClient::connect(grpc_url)
                .await?
                .delegator_delegations(cosmos::staking::v1beta1::QueryDelegatorDelegationsRequest {
                    delegator_addr: user.address.clone(),
                    pagination: None,
                })
                .await?
                .into_inner()
                .delegation_responses
                .into_iter()
                .try_fold(0u128, |total, del| -> Result<_, Error> {
                    let amount = del
                        .balance
                        .map(|coin| coin.amount.parse::<u128>())
                        .transpose()?
                        .unwrap_or(0);
                    Ok(total + amount)
                })?;

            let output = Output {
                address: user.address,
                sequence,
                airdrop_amount: user.amount,
                staked_amount,
            };

            let output_str = serde_json::to_string(&output)?;
            println!("{output_str}");

            // can i do this type annotation in a better way?
            Result::<_, Error>::Ok(output)
        }
    }))
    .await
    .unwrap();

    let output_str = serde_json::to_string_pretty(&output)?;
    fs::write(&cli.output, output_str)?;

    Ok(())
}
