use crate::configuration::all::BOOTSTRAP_SERVERS;
use crate::configuration::consumer::GROUP_ID;
use crate::wrap_err::KWResult;
use anyhow::anyhow;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::groups::GroupList;
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct KWConsumerConf {
    pub config: HashMap<String, String>,
    pub log_level: Option<RDKafkaLogLevel>,
    pub group_id: String,
    pub brokers: String,
    pub topics: Vec<String>,
}

impl KWConsumerConf {
    pub fn new(brokers: &str, group_id: &str) -> Self {
        Self {
            config: Default::default(),
            log_level: None,
            group_id: group_id.to_string(),
            brokers: brokers.to_string(),
            topics: vec![],
        }
    }

    pub fn set_topics<I, T>(mut self, topics: T) -> Self
    where
        I: AsRef<str>,
        T: IntoIterator<Item = I>,
    {
        self.topics = topics.into_iter().map(|x| x.as_ref().into()).collect();
        self
    }

    pub fn set_config<K, V>(mut self, config: HashMap<K, V>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        let config = config
            .into_iter()
            .fold(HashMap::new(), |mut map, (key, value)| {
                map.insert(key.into(), value.into());
                map
            });
        self.config = config;
        self
    }

    pub fn append_config<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.config.insert(key.into(), value.into());
        self
    }

    pub fn set_log_level(mut self, log_level: RDKafkaLogLevel) -> Self {
        self.log_level = Some(log_level);
        self
    }
}

pub struct KWConsumer {
    pub conf: KWConsumerConf,
    pub consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    pub admin_client: Mutex<Option<AdminClient<DefaultClientContext>>>,
}

impl KWConsumer {
    pub fn new(conf: KWConsumerConf) -> KWResult<Self> {
        let mut client = ClientConfig::new();
        client
            .set(GROUP_ID, &conf.group_id)
            .set(BOOTSTRAP_SERVERS, &conf.brokers);

        for (key, value) in &conf.config {
            client.set(key, value);
        }

        let consumer: StreamConsumer<_> = client
            .set_log_level(conf.log_level.unwrap_or(RDKafkaLogLevel::Warning))
            .create()?;
        Ok(Self {
            conf,
            consumer: Arc::new(consumer),
            admin_client: Default::default(),
        })
    }

    pub fn new_subscribe(conf: KWConsumerConf) -> KWResult<Self> {
        let kc = Self::new(conf)?;
        kc.subscribe()?;
        Ok(kc)
    }

    pub fn subscribe(&self) -> KWResult<()> {
        let topics: Vec<&str> = self.conf.topics.iter().map(|s| s.as_str()).collect();
        self.consumer.subscribe(&topics)?;
        info!("subscribe topics:{:?} success", topics);
        Ok(())
    }

    pub fn assign_split_partition_queue(
        &self,
        topic: &str,
        partition: i32,
    ) -> KWResult<Vec<StreamPartitionQueue<DefaultConsumerContext>>> {
        let mut list = TopicPartitionList::new();
        for i in 0..partition {
            list.add_partition_offset(topic, i, Offset::Stored)?;
        }

        self.consumer.assign(&list)?;

        let mut queues = vec![];
        for i in 0..partition {
            let q = self
                .consumer
                .split_partition_queue(topic, i)
                .ok_or_else(|| {
                    anyhow!(
                    "kafka split_partition_queue with topic:{topic},partition:{partition} error"
                )
                })?;
            queues.push(q)
        }

        Ok(queues)
    }

    pub async fn recv<'a>(&'_ self) -> Result<BorrowedMessage<'_>, KafkaError> {
        self.consumer.recv().await
    }

    pub fn unsubscribe(&mut self) {
        self.consumer.unsubscribe();
    }

    pub fn assign(&self, topic_name: &str, partition: i32) -> KWResult<()> {
        let mut list = TopicPartitionList::new();
        for i in 0..partition {
            list.add_partition_offset(topic_name, i, Offset::Stored)?;
        }
        Ok(self.consumer.assign(&list)?)
    }

    pub fn split_partition_queue(
        &self,
        topic: &str,
        partition: i32,
    ) -> KWResult<StreamPartitionQueue<DefaultConsumerContext>> {
        let partition_queue = self
            .consumer
            .split_partition_queue(topic, partition)
            .ok_or_else(|| {
                anyhow!(
                    "kafka split_partition_queue with topic:{topic},partition:{partition} error"
                )
            })?;
        Ok(partition_queue)
    }
    pub fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KWResult<()> {
        self.consumer.store_offset(topic, partition, offset)?;
        Ok(())
    }

    pub fn fetch_group_list(&self) -> KWResult<GroupList> {
        let list = self
            .consumer
            .fetch_group_list(None, Timeout::from(Duration::from_secs(5)))?;
        Ok(list)
    }
}
