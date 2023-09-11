use anyhow::Error as AnyhowErr;
use rdkafka::error::{KafkaError, RDKafkaError};

#[derive(thiserror::Error, Debug)]
pub enum KWError {
    #[error("rdkafka error")]
    RdKafka(#[from] RDKafkaError),
    #[error("kafka error")]
    Kafka(#[from] KafkaError),
    #[error("kafka-wrap error")]
    Normal(#[from] AnyhowErr),
}

pub type KWResult<T> = Result<T, KWError>;
