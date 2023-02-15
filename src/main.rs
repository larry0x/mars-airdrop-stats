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

    #[error(transparent)]
    Bech32(#[from] bech32::Error),

    #[error("Account not found: {address}")]
    AccountNotFound {
        address: String,
    },
}

#[derive(Parser)]
pub struct Cli {
    /// Input JSON file containing airdrop data
    #[arg(long)]
    pub input: Option<PathBuf>,

    /// Output file containing user sequence and staked amount
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// URL to a gRPC endpoint
    #[arg(long)]
    pub grpc_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let Cli {
        input,
        output,
        grpc_url,
    } = Cli::parse();

    let input_path = input.unwrap_or(PathBuf::from("./data/airdrop.json"));
    let output_path = output.unwrap_or(PathBuf::from("./data/output.json"));

    let input_str = fs::read_to_string(&input_path)?;
    let mut users: Vec<Input> = serde_json::from_str(&input_str)?;
    users.truncate(5);

    let output = future::try_join_all(users.into_iter().map(|user| {
        // https://stackoverflow.com/questions/66429545/clone-a-string-for-an-async-move-closure-in-rust
        let grpc_url = grpc_url.clone();
        async move {
            let (_, bytes, variant) = bech32::decode(&user.address)?;
            let address = bech32::encode("mars", bytes, variant)?;

            let sequence = cosmos::auth::v1beta1::query_client::QueryClient::connect(grpc_url.clone())
                .await?
                .account(cosmos::auth::v1beta1::QueryAccountRequest {
                    address: address.clone(),
                })
                .await?
                .into_inner()
                .account
                .as_ref()
                .map(<cosmos::auth::v1beta1::BaseAccount>::from_any)
                .transpose()?
                .ok_or_else(|| Error::AccountNotFound {
                    address: address.clone(),
                })?
                .sequence;

            let staked_amount = cosmos::staking::v1beta1::query_client::QueryClient::connect(grpc_url)
                .await?
                .delegator_delegations(cosmos::staking::v1beta1::QueryDelegatorDelegationsRequest {
                    delegator_addr: address.clone(),
                    pagination: None,
                })
                .await?
                .into_inner()
                .delegation_responses
                .into_iter()
                .try_fold(0u128, |mut total, del| -> Result<_, Error> {
                    if let Some(coin) = del.balance {
                        total += coin.amount.parse::<u128>()?;
                    }
                    Ok(total)
                })?;

            let output = Output {
                address,
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
    .await?;

    let output_str = serde_json::to_string_pretty(&output)?;
    fs::write(&output_path, output_str)?;

    Ok(())
}
