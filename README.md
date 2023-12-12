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
    activate 'Command Server'
    'Command Service Client'->>'Command Server': Create user 'Bob'
    deactivate 'Command Server'
    'Command Server'-->>'Event Listener A': User 'Bob' created
    'Command Server'-->>'Event Listener B': User 'Bob' created
```




# WARNING: Don't use in production


