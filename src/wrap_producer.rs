use crate::configuration::all::BOOTSTRAP_SERVERS;
use crate::hp_producer::HpProducer;
use crate::wrap_err::KWResult;

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseRecord, DefaultProducerContext};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::collections::HashMap;
use tokio::sync::Mutex;

pub struct KWProducerConf {
    pub config: HashMap<String, String>,
    pub log_level: Option<RDKafkaLogLevel>,
    pub brokers: String,
    pub topic: String,
    pub msg_timeout: Timeout,
}

impl KWProducerConf {
    pub fn new(brokers: &str, topic: &str) -> Self {
        Self {
            config: Default::default(),
            log_level: None,
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            msg_timeout: Timeout::Never,
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
}

pub struct KWProducer {
    pub conf: KWProducerConf,
    //TODO Context callback issue to be resolved
    pub producer: HpProducer<DefaultProducerContext>,
    pub admin_client: Mutex<Option<AdminClient<DefaultClientContext>>>,
}

impl KWProducer {
    pub fn new_hp(conf: KWProducerConf) -> KWResult<KWProducer> {
        let mut client = ClientConfig::new();
        client.set(BOOTSTRAP_SERVERS, &conf.brokers);

        for (key, value) in &conf.config {
            client.set(key, value);
        }
        let producer = client
            .set_log_level(conf.log_level.unwrap_or(RDKafkaLogLevel::Warning))
            .create()?;
        Ok(Self {
            conf,
            producer,
            admin_client: Default::default(),
        })
    }

    pub async fn send<'a>(
        &'a self,
        payload: &'a [u8],
        key: &'a [u8],
    ) -> Result<(), (KafkaError, BaseRecord<'a, [u8], [u8]>)> {
        let record = BaseRecord::to(&self.conf.topic).key(key).payload(payload);

        self.producer.send(record, self.conf.msg_timeout).await?;
        Ok(())
    }
}
