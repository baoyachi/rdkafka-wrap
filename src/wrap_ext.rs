use crate::configuration::all::BOOTSTRAP_SERVERS;
use crate::wrap_err::KWResult;
use crate::wrap_metadata::{MetadataTopicWrap, MetadataWrap};
use crate::{KWClient, KWConsumer, KWProducer, LogWrapExt};
use anyhow::anyhow;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{FromClientConfig, RDKafkaLogLevel};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext};
use std::sync::Arc;
use std::time::Duration;

pub(crate) type SafeAdminClient = Arc<AdminClient<DefaultClientContext>>;
pub type TopicName = String;
pub type Brokers<'a> = &'a str;

pub mod log_wrap {
    use rdkafka::config::RDKafkaLogLevel;

    pub trait LogWrapExt: Sized {
        type Item;
        fn default() -> Self::Item;
        fn get_or_init(&self) -> Self::Item;
    }

    impl LogWrapExt for RDKafkaLogLevel {
        type Item = Self;

        fn default() -> Self::Item {
            RDKafkaLogLevel::Warning
        }

        fn get_or_init(&self) -> Self::Item {
            *self
        }
    }

    impl LogWrapExt for Option<RDKafkaLogLevel> {
        type Item = RDKafkaLogLevel;

        fn default() -> Self::Item {
            RDKafkaLogLevel::default()
        }

        fn get_or_init(&self) -> Self::Item {
            self.unwrap_or(RDKafkaLogLevel::default())
        }
    }
}

pub trait AdminClientExt {
    fn create_admin_client<T: FromClientConfig>(&self) -> KWResult<T> {
        let admin_client = ClientConfig::new()
            .set(BOOTSTRAP_SERVERS, self.get_brokers())
            .set_log_level(self.get_log_level())
            .create()?;
        Ok(admin_client)
    }

    fn get_brokers(&self) -> &str;
    fn get_log_level(&self) -> RDKafkaLogLevel;
}

impl<'a> AdminClientExt for (Brokers<'a>, RDKafkaLogLevel) {
    fn get_brokers(&self) -> &str {
        self.0
    }

    fn get_log_level(&self) -> RDKafkaLogLevel {
        self.1
    }
}

#[async_trait::async_trait]
pub trait OptionExt {
    type AdminClient: FromClientConfig;
    async fn create_topic<'a, I>(&self, topics: I) -> KWResult<()>
    where
        I: IntoIterator<Item = &'a NewTopic<'a>> + Send,
    {
        let admin_client = self.get_admin_client().await?;

        let topics_result = admin_client
            .create_topics(topics, &AdminOptions::new())
            .await?;

        let mut err_msg = vec![];
        for ret in topics_result {
            match ret {
                Ok(_) => {}
                Err((topic, err_code)) => {
                    // topic already exists
                    if let RDKafkaErrorCode::TopicAlreadyExists = err_code {
                        warn!("create kafka topic {}: topic already exists", topic);
                    } else {
                        err_msg.push(format!("topic {} -> error code: {}", topic, err_code))
                    }
                }
            }
        }

        if err_msg.is_empty() {
            return Ok(());
        }
        Err(anyhow!("failed to create kafka:{}", err_msg.join(",")).into())
    }

    fn create_admin_client(&self) -> KWResult<Self::AdminClient> {
        let ext = (self.get_brokers(), self.get_log_level());
        ext.create_admin_client()
    }

    fn get_brokers(&self) -> &str;
    fn get_log_level(&self) -> RDKafkaLogLevel;

    async fn get_admin_client(&self) -> KWResult<SafeAdminClient>;

    async fn get_topics<C: ClientContext>(&self) -> KWResult<Vec<MetadataTopicWrap>> {
        let metadata: MetadataWrap = self
            .get_admin_client()
            .await?
            .inner()
            .fetch_metadata(None, Timeout::from(Duration::from_secs(5)))?
            .into();
        Ok(metadata.topics)
    }
}

#[async_trait::async_trait]
impl OptionExt for KWProducer {
    type AdminClient = AdminClient<DefaultClientContext>;

    fn get_brokers(&self) -> &str {
        self.conf.brokers.as_str()
    }

    fn get_log_level(&self) -> RDKafkaLogLevel {
        self.conf.log_level.get_or_init()
    }

    async fn get_admin_client(&self) -> KWResult<SafeAdminClient> {
        let mut guard = self.admin_client.lock().await;
        if guard.is_none() {
            let client = self.create_admin_client()?;
            *guard = Some(Arc::new(client));
        }

        Ok(guard.clone().unwrap())
    }
}

#[async_trait::async_trait]
impl OptionExt for KWConsumer {
    type AdminClient = AdminClient<DefaultClientContext>;

    fn get_brokers(&self) -> &str {
        self.conf.brokers.as_str()
    }

    fn get_log_level(&self) -> RDKafkaLogLevel {
        self.conf.log_level.get_or_init()
    }

    async fn get_admin_client(&self) -> KWResult<SafeAdminClient> {
        let mut guard = self.admin_client.lock().await;
        if guard.is_none() {
            let client = self.create_admin_client()?;
            *guard = Some(Arc::new(client));
        }

        Ok(guard.clone().unwrap())
    }
}

#[async_trait::async_trait]
impl OptionExt for KWClient {
    type AdminClient = AdminClient<DefaultClientContext>;

    fn get_brokers(&self) -> &str {
        self.brokers.as_str()
    }

    fn get_log_level(&self) -> RDKafkaLogLevel {
        self.get_log_level()
    }

    async fn get_admin_client(&self) -> KWResult<SafeAdminClient> {
        Ok(self.admin_client.clone())
    }
}
