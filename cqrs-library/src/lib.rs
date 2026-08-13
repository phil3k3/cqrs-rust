//! # CQRS Library
//!
//! A Rust implementation of the CQRS (Command Query Responsibility Segregation) pattern
//! with support for event sourcing and pluggable transport layers.
//!
//! ## Overview
//!
//! This library provides the core abstractions and implementations for building CQRS-based
//! systems in Rust. It separates commands (write operations) from events (notifications of
//! state changes) and supports asynchronous message processing.
//!
//! ## Core Concepts
//!
//! - **Commands**: Requests to perform actions that result in state changes
//! - **Events**: Immutable records of things that have happened
//! - **Command Handlers**: Functions that process commands and produce events
//! - **Event Handlers**: Functions that react to events (e.g., updating read models)
//! - **Transport**: Abstraction for message delivery (e.g., Kafka, in-memory channels)
//!
//! ## Quick Start
//!
//! ```ignore
//! use cqrs_library::prelude::*;
//! use cqrs_library::cqrs::{Command, CommandEnvelope, EventEnvelope, CommandServiceServer};
//!
//! // Define a command
//! #[derive(Serialize, Deserialize)]
//! struct CreateUserCommand {
//!     user_id: String,
//!     name: String,
//! }
//!
//! impl Command for CreateUserCommand {
//!     fn get_subject(&self) -> String { self.user_id.clone() }
//!     fn get_type(&self) -> String { "CreateUserCommand".to_string() }
//! }
//!
//! // Define an event
//! #[derive(Debug, Serialize, Deserialize)]
//! struct UserCreatedEvent {
//!     user_id: String,
//!     name: String,
//! }
//!
//! // Close over every command/event type this service handles, so the
//! // compiler enforces every variant is matched.
//! #[derive(Deserialize)]
//! #[serde(tag = "type", content = "payload")]
//! enum AppCommand {
//!     CreateUserCommand(CreateUserCommand),
//! }
//! impl CommandEnvelope for AppCommand {
//!     fn subject(&self) -> String { match self { Self::CreateUserCommand(c) => c.get_subject() } }
//!     fn command_type(&self) -> &'static str { match self { Self::CreateUserCommand(_) => "CreateUserCommand" } }
//! }
//!
//! #[derive(Serialize)]
//! #[serde(tag = "type")]
//! enum DomainEvent {
//!     UserCreatedEvent(UserCreatedEvent),
//! }
//! impl EventEnvelope for DomainEvent {
//!     fn id(&self) -> String { match self { Self::UserCreatedEvent(e) => e.user_id.clone() } }
//!     fn event_type(&self) -> &'static str { match self { Self::UserCreatedEvent(_) => "UserCreatedEvent" } }
//! }
//!
//! // Create server with a single dispatch handler
//! let server = CommandServiceServer::new("user-service", &transport, handle_command);
//! ```
//!
//! ## Architecture
//!
//! The library follows a layered architecture:
//!
//! 1. **Domain Layer**: Commands and Events (defined by application)
//! 2. **Application Layer**: Command and Event handlers (defined by application)
//! 3. **Infrastructure Layer**: Transport implementations (e.g., `cqrs-kafka`)
//!
//! ## Features
//!
//! - Async/await support with Tokio
//! - Pluggable transport layer
//! - Type-safe command and event handling
//! - Lazy command deserialization for efficiency
//! - Correlation ID tracking for request/response patterns

pub mod cqrs;
pub mod error;
pub mod prelude;
