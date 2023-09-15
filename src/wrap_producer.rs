use crate::configuration::all::BOOTSTRAP_SERVERS;
use crate::hp_producer::HpProducer;
use crate::wrap_err::KWResult;

use crate::wrap_ext::SafeAdminClient;
use crate::{KWError, LogWrapExt, OptionExt};
use anyhow::anyhow;
use rdkafka::admin::NewTopic;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::message::ToBytes;
use rdkafka::producer::{BaseRecord, DefaultProducerContext};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::collections::HashMap;
use tokio::sync::Mutex;

pub struct KWProducerConf {
    pub config: HashMap<String, String>,
    pub log_level: Option<RDKafkaLogLevel>,
    pub brokers: String,
    pub msg_timeout: Timeout,
    pub create_topic_conf: Option<NewTopic<'static>>,
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
            create_topic_conf: None,
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

    pub fn set_create_topic_conf<'a>(mut self, topic: NewTopic<'a>) -> Self {
        let topic = unsafe { std::mem::transmute::<NewTopic<'a>, NewTopic<'_>>(topic) };
        self.create_topic_conf = Some(topic);
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

    pub async fn send<'a, K, P>(
        &'a self,
        record: BaseRecord<'a, K, P>,
    ) -> Result<(), (KWError, Option<BaseRecord<'a, K, P>>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let topic = record.topic;
        match self.producer.send(record, self.conf.msg_timeout).await {
            Ok(_) => {
                return Ok(());
            }
            Err((e, record)) => {
                if self.conf.create_topic_conf.is_some()
                    && e == KafkaError::MessageProduction(RDKafkaErrorCode::UnknownTopic)
                {
                    let topic = self
                        .conf
                        .create_topic_conf
                        .as_ref()
                        .ok_or_else(|| (anyhow!("lost topic:{} config", topic).into(), None))?;
                    self.create_topic([topic]).await.map_err(|e| (e, None))?;
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
}
