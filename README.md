# cqrs-rust

A simple command server/command service client written in Rust.

Requirements:
* cargo
* cmake (for librdkafka)
* protoc

Visual example:

```mermaid
sequenceDiagram
    participant 'Command Server'
    participant 'Command Service Client'
    participant 'Event Listener A'
    participant 'Event Listener B'
    'Command Server'->>'Command Service Client': Create user 'Bob'
    John-->>Alice: Great!
    John->>Bob: How about you?
    Bob-->>John: Jolly good!
```




# WARNING: Don't use in production


