use rdkafka::client::Client;
use rdkafka::config::{FromClientConfig, FromClientConfigAndContext};
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::{DeliveryResult, OwnedMessage, ToBytes};
use rdkafka::producer::{
    BaseProducer, BaseRecord, DefaultProducerContext, NoCustomPartitioner, Partitioner, Producer,
    ProducerContext, PurgeConfig,
};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, TopicPartitionList};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct HpProducerContext<F> {
    context_type: F,
}

impl<F> ClientContext for HpProducerContext<F> where F: Send + Sync {}

impl<F> ProducerContext for HpProducerContext<F>
where
    F: Fn(KafkaError, OwnedMessage) + Send + Sync,
{
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result {
            Ok(_) => {}
            Err((e, record)) => (self.context_type)(e.clone(), record.detach()),
        }
    }
}

/// A low-level Kafka producer with a separate thread for event handling.
///
/// The `HpProducer` is a [`BaseProducer`] with a separate thread
/// dedicated to calling `poll` at regular intervals in order to execute any
/// queued events, such as delivery notifications. The thread will be
/// automatically stopped when the producer is dropped.
#[must_use = "The High Performance producer will stop immediately if unused"]
pub struct HpProducer<C, Part: Partitioner = NoCustomPartitioner>
where
    C: ProducerContext<Part> + 'static,
{
    producer: Arc<BaseProducer<C, Part>>,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl FromClientConfig for HpProducer<DefaultProducerContext, NoCustomPartitioner> {
    fn from_config(config: &ClientConfig) -> KafkaResult<HpProducer<DefaultProducerContext>> {
        HpProducer::from_config_and_context(config, DefaultProducerContext)
    }
}

impl<C, Part> FromClientConfigAndContext<C> for HpProducer<C, Part>
where
    Part: Partitioner + Send + Sync + 'static,
    C: ProducerContext<Part> + 'static,
{
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<HpProducer<C, Part>> {
        let producer = Arc::new(BaseProducer::from_config_and_context(config, context)?);
        let should_stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let producer = Arc::clone(&producer);
            let should_stop = should_stop.clone();
            thread::Builder::new()
                .name("producer polling thread".to_string())
                .spawn(move || {
                    trace!("Polling thread loop started");
                    loop {
                        let n = producer.poll(Duration::from_millis(100));
                        if n == 0 {
                            if should_stop.load(Ordering::Relaxed) {
                                // We received nothing and the thread should
                                // stop, so break the loop.
                                break;
                            }
                        } else {
                            trace!("Received {} events", n);
                        }
                    }
                    trace!("Polling thread loop terminated");
                })
                .expect("Failed to start polling thread")
        };
        Ok(HpProducer {
            producer,
            should_stop,
            handle: Some(thread),
        })
    }
}

impl<C, Part> HpProducer<C, Part>
where
    Part: Partitioner,
    C: ProducerContext<Part> + 'static,
{
    /// Sends a message to Kafka.
    ///
    /// See the documentation for [`BaseProducer::send`] for details.
    // Simplifying the return type requires generic associated types, which are
    // unstable.
    pub async fn send<'a, K, P, T>(
        &self,
        record: BaseRecord<'a, K, P, C::DeliveryOpaque>,
        queue_timeout: T,
    ) -> Result<(), (KafkaError, BaseRecord<'a, K, P, C::DeliveryOpaque>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
        T: Into<Timeout>,
    {
        let start_time = Instant::now();
        let queue_timeout = queue_timeout.into();
        let can_retry = || match queue_timeout {
            Timeout::Never => true,
            Timeout::After(t) if start_time.elapsed() < t => true,
            _ => false,
        };
        let mut base_record = record;
        loop {
            match self.producer.send(base_record) {
                Err((e, record))
                    if e == KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
                        && can_retry() =>
                {
                    //need retry
                    base_record = record;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok(_) => break Ok(()),

                Err(e) => {
                    break Err(e);
                }
            }
        }
    }

    /// Polls the internal producer.
    ///
    /// This is not normally required since the `HpProducer` has a thread
    /// dedicated to calling `poll` regularly.
    pub fn poll<T: Into<Timeout>>(&self, timeout: T) {
        self.producer.poll(timeout);
    }
}

impl<C, Part> Producer<C, Part> for HpProducer<C, Part>
where
    Part: Partitioner,
    C: ProducerContext<Part> + 'static,
{
    fn client(&self) -> &Client<C> {
        self.producer.client()
    }

    fn flush<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.flush(timeout)
    }

    fn purge(&self, flags: PurgeConfig) {
        self.producer.purge(flags)
    }

    fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    fn init_transactions<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.init_transactions(timeout)
    }

    fn begin_transaction(&self) -> KafkaResult<()> {
        self.producer.begin_transaction()
    }

    fn send_offsets_to_transaction<T: Into<Timeout>>(
        &self,
        offsets: &TopicPartitionList,
        cgm: &ConsumerGroupMetadata,
        timeout: T,
    ) -> KafkaResult<()> {
        self.producer
            .send_offsets_to_transaction(offsets, cgm, timeout)
    }

    fn commit_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.commit_transaction(timeout)
    }

    fn abort_transaction<T: Into<Timeout>>(&self, timeout: T) -> KafkaResult<()> {
        self.producer.abort_transaction(timeout)
    }
}

impl<C, Part> Drop for HpProducer<C, Part>
where
    Part: Partitioner,
    C: ProducerContext<Part> + 'static,
{
    fn drop(&mut self) {
        trace!("Destroy HpProducer");
        if let Some(handle) = self.handle.take() {
            trace!("Stopping polling");
            self.should_stop.store(true, Ordering::Relaxed);
            trace!("Waiting for polling thread termination");
            match handle.join() {
                Ok(()) => trace!("Polling stopped"),
                Err(e) => warn!("Failure while terminating thread: {:?}", e),
            };
        }
        trace!("HpProducer destroyed");
    }
}
