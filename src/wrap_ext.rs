use crate::configuration::all::BOOTSTRAP_SERVERS;
use crate::wrap_err::KWResult;
use crate::{KWConsumer, KWProducer};
use anyhow::anyhow;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{FromClientConfig, RDKafkaLogLevel};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::ClientConfig;

#[async_trait::async_trait]
pub trait OptionExt {
    type AdminClient: FromClientConfig;
    async fn create_topic<'a, I>(&self, topics: I) -> KWResult<()>
    where
        I: IntoIterator<Item = &'a NewTopic<'a>> + Send;

    fn create_admin_client(&self) -> KWResult<Self::AdminClient> {
        let admin_client = ClientConfig::new()
            .set(BOOTSTRAP_SERVERS, self.get_brokers())
            .set_log_level(self.get_log_level())
            .create()?;
        Ok(admin_client)
    }

    fn get_brokers(&self) -> &str;
    fn get_log_level(&self) -> RDKafkaLogLevel;
}

#[async_trait::async_trait]
impl OptionExt for KWProducer {
    type AdminClient = AdminClient<DefaultClientContext>;

    async fn create_topic<'a, I>(&self, topics: I) -> KWResult<()>
    where
        I: IntoIterator<Item = &'a NewTopic<'a>> + Send,
    {
        let mut guard = self.admin_client.lock().await;
        if guard.is_none() {
            let client = self.create_admin_client()?;
            *guard = Some(client);
        }

        let admin_client = guard.as_ref().unwrap();

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

    fn get_brokers(&self) -> &str {
        self.conf.brokers.as_str()
    }

    fn get_log_level(&self) -> RDKafkaLogLevel {
        self.conf.log_level.unwrap_or(RDKafkaLogLevel::Error)
    }
}

#[async_trait::async_trait]
impl OptionExt for KWConsumer {
    type AdminClient = AdminClient<DefaultClientContext>;

    async fn create_topic<'a, I>(&self, topics: I) -> KWResult<()>
    where
        I: IntoIterator<Item = &'a NewTopic<'a>> + Send,
    {
        let mut guard = self.admin_client.lock().await;
        if guard.is_none() {
            let client = self.create_admin_client()?;
            *guard = Some(client);
        }

        let admin_client = guard.as_ref().unwrap();

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

    fn get_brokers(&self) -> &str {
        self.conf.brokers.as_str()
    }

    fn get_log_level(&self) -> RDKafkaLogLevel {
        self.conf.log_level.unwrap_or(RDKafkaLogLevel::Error)
    }
}
