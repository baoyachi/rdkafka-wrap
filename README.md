# rdkafka-wrap

[docsrs]: https://docs.rs/rdkafka-wrap

[![GitHub Actions](https://github.com/baoyachi/rdkafka-wrap/workflows/check/badge.svg)](https://github.com/baoyachi/rdkafka-wrap/actions?query=workflow%3Abuild)
[![Crates.io](https://img.shields.io/crates/v/rdkafka-wrap.svg)](https://crates.io/crates/rdkafka-wrap)
[![Docs.rs](https://docs.rs/rdkafka-wrap/badge.svg)](https://docs.rs/rdkafka-wrap)
[![Download](https://img.shields.io/crates/d/rdkafka-wrap)](https://crates.io/crates/rdkafka-wrap)

"What is rdkfafka-wrap?"

[rdkfafka-wrap](https://github.com/baoyachi/rdkafka-wrap) :as the name suggests, it is a wrapper for [rdkafka](https://crates.io/crates/rdkafka), offering a set of convenient and practical features built on top of it.


## Highlighted features
* 100% compatible with [rdkafka](https://crates.io/crates/rdkafka)
* Supporting the functionality of HpProducer, harnessing the dual advantages of ThreadedProducer and FutureProducer, enhances performance and strengthens retry capabilities.
* Supporting out-of-the-box shortcuts, eliminating the need for complex rdkafka configurations.
* Supporting serialization and deserialization of results obtained from key objects without the need to repackage the original rdkafka objects.
* Supporting for automatic topic creation.
* Supports the separation and combined usage of producer and consumer objects.
* Supports configuration transparency for producer and consumer objects, making it easy to access.


## Hotly anticipated features coming soon...
- [ ] Support out-of-the-box operations for popular web frameworks to access key Kafka information.
- [ ] Support the functionality to assess the production and consumption efficiency of Kafka operations on the current machine.
- [ ] Support rate limiting functionality.
- [ ] Support callback functionality for HpProducerContext.