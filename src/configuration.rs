/// All configuration reference from :https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

pub mod all {
    pub const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
}

pub mod producer {}

pub mod consumer {
    pub const GROUP_ID: &str = "group.id";
}
