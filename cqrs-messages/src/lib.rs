pub mod cqrs {
    pub mod messages {
        include!(concat!(env!("OUT_DIR"), "/cqrs.rs"));
    }
}