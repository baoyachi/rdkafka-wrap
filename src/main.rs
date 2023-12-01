use log::{info, warn};
use rdkafka::producer::BaseRecord;
use rdkafka::util::Timeout;
use rdkafka_wrap::{KWConsumer, KWConsumerConf, KWProducer, KWProducerConf};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

// const BROKERS: &str = "localhost:9092";

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    simple_log::quick!();


    let args: Vec<_> = std::env::args().collect();

    let BROKERS = args.get(1).unwrap();
    let topic = args.get(2).unwrap();
    let timeout_duration = args.get(3).map(|x| x.parse::<u64>().unwrap_or(20)).unwrap_or(20);
    info!("brokers:{}",BROKERS);
    info!("topic:{}",topic);

    let topic = "test_kaka";
    let count = 10;

    tokio::spawn(async move {
        let conf = KWProducerConf {
            config: Default::default(),
            log_level: None,
            brokers: BROKERS.to_string(),
            msg_timeout: Timeout::Never,
            topic: Some(topic.into()),
            num_partitions: 1,
            replication: 1,
        };

        let producer = KWProducer::new(conf).unwrap();
        let mut index = 0;
        loop {
            if index >= count {
                producer.flush(None).unwrap();
                info!("send flush");
                break;
            }
            producer
                .send(BaseRecord::to(topic).payload(b"hello").key(""))
                .await
                .unwrap();
            index += 1;
            info!("send index:{}", index);
        }
    });

    let conf = KWConsumerConf {
        config: HashMap::from([
            ("enable.partition.eof".into(), "false".into()),
            ("enable.auto.commit".into(), "true".into()),
            ("enable.auto.offset.store".into(), "true".into()),
            ("receive.message.max.bytes".into(), "100001000".into()),
            ("auto.offset.reset".into(), " earliest".into()),
            // ("heartbeat.interval.ms".into(), "1000".into()),
            // ("session.timeout.ms".into(), " 6000".into()),
        ]),
        log_level: None,
        group_id: "kw_test".to_string(),
        brokers: BROKERS.to_string(),
        topics: vec![topic.into()],
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    let consumer = KWConsumer::new(conf).unwrap();
    consumer.unsubscribe();
    consumer.subscribe().unwrap();
    let mut index = 0;
    loop {
        match timeout(Duration::from_secs(timeout_duration), consumer.recv()).await {
            Ok(t) => {
                let _ = t.unwrap();
                index += 1;
                info!("rev index:{}", index);
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
