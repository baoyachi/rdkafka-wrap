use crate::wrap_ext::SafeAdminClient;
use crate::{AdminClientExt, GroupID, KWConsumer, KWProducer, KWResult, LogWrapExt, TopicName};
use rdkafka::admin::{AdminClient, NewTopic};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::RDKafkaLogLevel;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub struct KWClient {
    pub admin_client: SafeAdminClient,
    pub kw_producer: KWProducer,
    pub kw_consumer: BTreeMap<GroupID, KWConsumer>,
    log_level: RDKafkaLogLevel,
    pub brokers: String,
    pub create_topic_conf: HashMap<TopicName, NewTopic<'static>>,
}

impl KWClient {
    fn new<B>(
        brokers: B,
        kw_producer: KWProducer,
        kw_consumer: KWConsumer,
        log_level: Option<RDKafkaLogLevel>,
    ) -> KWResult<Self>
    where
        B: AsRef<str>,
    {
        let log_level = log_level.get_or_init();
        let brokers = brokers.as_ref();
        let mut consumer = BTreeMap::default();
        consumer.insert(kw_consumer.get_group_id().to_string(), kw_consumer);

        let admin_client =
            (brokers, log_level).create_admin_client::<AdminClient<DefaultClientContext>>()?;
        let client = Self {
            admin_client: Arc::new(admin_client),
            kw_producer,
            kw_consumer: consumer,
            create_topic_conf: Default::default(),
            log_level,
            brokers: brokers.to_string(),
        };
        Ok(client)
    }

    pub fn set_consumer(mut self, kw_consumer: KWConsumer) -> Self {
        self.kw_consumer
            .insert(kw_consumer.get_group_id().to_string(), kw_consumer);
        self
    }

    pub fn set_log_level(mut self, log_level: RDKafkaLogLevel) -> Self {
        self.kw_producer.conf.log_level = Some(log_level);
        for consumer in self.kw_consumer.values_mut() {
            consumer.conf.log_level = Some(log_level);
        }
        self.log_level = log_level;
        self
    }

    pub fn get_log_level(&self) -> RDKafkaLogLevel {
        self.log_level
    }
}
