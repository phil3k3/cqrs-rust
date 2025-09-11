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
    'Command Service Client'->>'Command Server': Create user 'Bob'
    activate 'Command Server'
    'Command Server'->>'Command Service Client': Ok
    deactivate 'Command Server'
    'Command Server'-->>'Event Listener A': User 'Bob' created
    'Command Server'-->>'Event Listener B': User 'Bob' created
```

As this project is still early stage, it's advised not to use it in production-grade environments. 
Contributions are welcome.

# Future improvements
* Introduce a state store to allow for snaphot based recovery of the command server state
* Improve the developer experience
* Fully introduce Rust-style error handling
  

