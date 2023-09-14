use rdkafka::error::RDKafkaErrorCode;
use rdkafka::metadata::{Metadata, MetadataBroker, MetadataPartition, MetadataTopic};

pub struct MetadataWrap {
    pub orig_broker_id: i32,
    pub orig_broker_name: String,
    pub brokers: Vec<MetadataBrokerWrap>,
    pub topics: Vec<MetadataTopicWrap>,
}

impl From<Metadata> for MetadataWrap {
    fn from(value: Metadata) -> Self {
        Self {
            orig_broker_id: value.orig_broker_id(),
            orig_broker_name: value.orig_broker_name().to_string(),
            brokers: value.brokers().iter().map(|x| x.into()).collect(),
            topics: value.topics().iter().map(|x| x.into()).collect(),
        }
    }
}

pub struct MetadataBrokerWrap {
    pub id: i32,
    pub host: String,
    pub port: String,
}

impl<'a> From<&'a MetadataBroker> for MetadataBrokerWrap {
    fn from(value: &'a MetadataBroker) -> Self {
        Self {
            id: value.id(),
            host: value.host().to_string(),
            port: value.port().to_string(),
        }
    }
}

pub struct MetadataTopicWrap {
    pub name: String,
    pub partitions: Vec<MetadataPartitionWrap>,
    pub error: Option<RDKafkaErrorCode>,
}

impl<'a> From<&'a MetadataTopic> for MetadataTopicWrap {
    fn from(value: &'a MetadataTopic) -> Self {
        Self {
            name: value.name().to_string(),
            partitions: value.partitions().iter().map(|x| x.into()).collect(),
            error: value.error().map(|x| x.into()),
        }
    }
}

pub struct MetadataPartitionWrap {
    // the id of the partition.
    pub id: i32,
    // the broker id of the leader broker for the partition.
    pub leader: i32,
    // the metadata error for the partition, or None if there is no error.
    pub error: Option<RDKafkaErrorCode>,
    // the broker IDs of the replicas.
    pub replicas: Vec<i32>,
    // the broker IDs of the in-sync replicas.
    pub isr: Vec<i32>,
}

impl<'a> From<&'a MetadataPartition> for MetadataPartitionWrap {
    fn from(value: &'a MetadataPartition) -> Self {
        Self {
            id: value.id(),
            leader: value.leader(),
            error: value.error().map(|x| x.into()),
            replicas: value.replicas().to_vec(),
            isr: value.isr().to_vec(),
        }
    }
}
