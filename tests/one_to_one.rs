use rdkafka::admin::{NewTopic, TopicReplication};
use rdkafka::producer::{BaseRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka_wrap::{KWConsumer, KWConsumerConf, KWProducer, KWProducerConf};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

const BROKERS: &str = "localhost:9092";

#[tokio::test(flavor = "multi_thread")]
async fn main() {
    simple_log::quick!();
    let topic = "test_kaka";
    let count = 10;

    tokio::spawn(async move {
        let conf = KWProducerConf {
            config: Default::default(),
            log_level: None,
            brokers: BROKERS.to_string(),
            msg_timeout: Timeout::Never,
            create_topic_conf: Some(NewTopic {
                name: "test_kaka",
                num_partitions: 1,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }),
        };

        let producer = KWProducer::new(conf).unwrap();
        let mut index = 0;
        loop {
            if index >= count {
                producer.producer.flush(None).unwrap();
                break;
            }
            producer
                .send(BaseRecord::to(topic).payload(b"hello").key(""))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            index += 1;
        }
    });

    let conf = KWConsumerConf {
        config: HashMap::from([
            ("enable.partition.eof".into(), "false".into()),
            ("enable.auto.commit".into(), "true".into()),
            ("enable.auto.offset.store".into(), "true".into()),
            ("receive.message.max.bytes".into(), "100001000".into()),
            ("auto.offset.reset".into(), " latest".into()),
        ]),
        log_level: None,
        group_id: "kw_test".to_string(),
        brokers: BROKERS.to_string(),
        topics: vec![topic.into()],
    };
    let consumer = KWConsumer::new_subscribe(conf).unwrap();
    let mut index = 0;
    loop {
        match timeout(Duration::from_secs(3), consumer.recv()).await {
            Ok(t) => {
                let _ = t.unwrap();
                index += 1;
            }
            Err(_) => {
                if index != 0 {
                    break;
                }
            }
        }
    }
    assert_eq!(index, count);
}
