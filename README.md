# cqrs-rust

A simple command server/command service client written in Rust.

Requirements:
* cargo
* cmake (for librdkafka)
* protoc

Visual example:

```mermaid
sequenceDiagram
    participant Command server
    participant Command service client
    participant Event listener A
    participant Event listener B
    Command service client->>Command server: Create user 'bob'
    Validation and processing
    Command server-->>Command service client: Ok
    Command server->>Event listener A: User 'bob' created
    Command server->>Event listener B: User 'bob' created
```

# WARNING: Don't use in production


