use crate::configuration::all::BOOTSTRAP_SERVERS;
use crate::hp_producer::HpProducer;
use crate::wrap_err::KWResult;

use crate::wrap_ext::SafeAdminClient;
use crate::{KWError, LogWrapExt, OptionExt};
use anyhow::anyhow;
use rdkafka::admin::{NewTopic, TopicReplication};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::message::ToBytes;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::collections::HashMap;
use tokio::sync::Mutex;

const CONF_NO_TOPIC: &str = "conf no topic";

#[derive(Debug)]
pub struct KWProducerConf {
    pub config: HashMap<String, String>,
    pub log_level: Option<RDKafkaLogLevel>,
    pub brokers: String,
    pub msg_timeout: Timeout,
    pub topic: Option<String>,
    pub num_partitions: i32,
    pub replication: i32,
}

impl KWProducerConf {
    pub fn new<B>(brokers: B) -> Self
    where
        B: AsRef<str>,
    {
        Self {
            config: Default::default(),
            log_level: None,
            brokers: brokers.as_ref().to_string(),
            msg_timeout: Timeout::Never,
            topic: None,
            num_partitions: 1,
            replication: 1,
        }
    }

    pub fn set_timeout<T: Into<Timeout>>(mut self, msg_timeout: T) {
        self.msg_timeout = msg_timeout.into();
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

    pub fn set_create_topic_conf<'a>(
        mut self,
        topic: impl Into<String>,
        num_partitions: i32,
        replication: i32,
    ) -> Self {
        self.topic = Some(topic.into());
        self.num_partitions = num_partitions;
        self.replication = replication;
        self
    }
}

pub struct KWProducer {
    pub conf: KWProducerConf,
    //TODO Context callback issue to be resolved
    pub producer: HpProducer<DefaultProducerContext>,
    pub admin_client: Mutex<Option<SafeAdminClient>>,
}

impl KWProducer {
    pub fn new(conf: KWProducerConf) -> KWResult<KWProducer> {
        let mut client = ClientConfig::new();
        client.set(BOOTSTRAP_SERVERS, &conf.brokers);

        for (key, value) in &conf.config {
            client.set(key, value);
        }
        let producer = client
            .set_log_level(conf.log_level.get_or_init())
            .create()?;
        Ok(Self {
            conf,
            producer,
            admin_client: Default::default(),
        })
    }

    pub fn new_topic(&self) -> KWResult<NewTopic> {
        let topic = self
            .conf
            .topic
            .as_ref()
            .ok_or_else(|| anyhow!(CONF_NO_TOPIC))?;
        let replica = TopicReplication::Fixed(self.conf.replication);
        Ok(NewTopic::new(topic, self.conf.num_partitions, replica))
    }

    pub async fn send<'a, K, P>(
        &'a self,
        record: BaseRecord<'a, K, P>,
    ) -> Result<(), (KWError, Option<BaseRecord<'a, K, P>>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        match self.producer.send(record, self.conf.msg_timeout).await {
            Ok(_) => {
                return Ok(());
            }
            Err((e, record)) => {
                if self.conf.topic.is_some()
                    && e == KafkaError::MessageProduction(RDKafkaErrorCode::UnknownTopic)
                {
                    let topic = self.new_topic().map_err(|e| (e, None))?;
                    self.create_topic([&topic]).await.map_err(|e| (e, None))?;
                    self.producer
                        .send(record, self.conf.msg_timeout)
                        .await
                        .map_err(|(e, record)| (e.into(), Some(record)))?;
                } else {
                    return Err((e.into(), Some(record)));
                }
            }
        }
        Ok(())
    }

    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KWResult<()> {
        self.producer.flush(timeout)?;
        Ok(())
    }
}
