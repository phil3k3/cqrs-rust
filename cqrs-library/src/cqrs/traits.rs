use crate::cqrs::CommandServiceServer;
use crate::prelude::*;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Implemented by a service's closed command enum, wrapping every command
/// type that service accepts.
///
/// The framework uses this to extract routing/response metadata from a
/// freshly-deserialized command without knowing its concrete variant.
///
/// # Examples
///
/// ```ignore
/// use serde::Deserialize;
/// use cqrs_library::cqrs::traits::{Command, CommandEnvelope};
///
/// #[derive(Deserialize)]
/// #[serde(tag = "type", content = "payload")]
/// enum AppCommand {
///     CreateUserCommand(CreateUserCommand),
/// }
///
/// impl CommandEnvelope for AppCommand {
///     fn subject(&self) -> String {
///         match self {
///             Self::CreateUserCommand(c) => c.get_subject(),
///         }
///     }
///
///     fn command_type(&self) -> &'static str {
///         match self {
///             Self::CreateUserCommand(_) => "CreateUserCommand",
///         }
///     }
/// }
/// ```
pub trait CommandEnvelope: DeserializeOwned {
    /// Returns the subject (aggregate ID) of the wrapped command.
    fn subject(&self) -> String;

    /// Returns the command type identifier of the wrapped command.
    fn command_type(&self) -> &'static str;

    /// Returns the version of the wrapped command's schema.
    ///
    /// Defaults to 1. Override to support command schema evolution.
    fn version(&self) -> i32 {
        1
    }
}

/// Implemented by a service's closed event enum, wrapping every event type
/// that service can produce.
///
/// # Examples
///
/// ```ignore
/// use serde::Serialize;
/// use cqrs_library::cqrs::traits::EventEnvelope;
///
/// #[derive(Serialize)]
/// #[serde(tag = "type")]
/// enum DomainEvent {
///     UserCreatedEvent(UserCreatedEvent),
/// }
///
/// impl EventEnvelope for DomainEvent {
///     fn id(&self) -> String {
///         match self {
///             Self::UserCreatedEvent(e) => e.user_id.clone(),
///         }
///     }
///
///     fn event_type(&self) -> &'static str {
///         match self {
///             Self::UserCreatedEvent(_) => "UserCreatedEvent",
///         }
///     }
/// }
/// ```
pub trait EventEnvelope: Serialize {
    /// Returns the unique identifier for this event's aggregate.
    fn id(&self) -> String;

    /// Returns the event type identifier.
    fn event_type(&self) -> &'static str;

    /// Returns the version of this event schema.
    ///
    /// Defaults to 1. Override this method to support event schema evolution.
    fn version(&self) -> i32 {
        1
    }
}

/// Produces domain events to the event store or message bus.
///
/// Implemented by components that can publish events, typically the command
/// service server. `Event` is an associated type rather than a generic
/// method so `&dyn EventProducer<Event = E>` remains usable as a trait
/// object — handlers take a trait object so they stay easy to unit test
/// against a fake producer without spinning up a real [`Transport`].
pub trait EventProducer {
    /// The closed event enum this producer publishes.
    type Event;

    /// Publishes a domain event.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be serialized or sent.
    fn produce(&self, event: Self::Event) -> Result<()>;
}

/// Sends messages to an outbound channel.
///
/// This is a simplified interface for sending raw byte messages with an optional key,
/// typically used for commands sent from the client.
pub trait OutboundChannel: Send + Sync {
    /// Sends a message with the given key.
    ///
    /// The key is used for partitioning in distributed message systems like Kafka.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be sent.
    fn send(&self, key: &[u8], message: &[u8]) -> Result<()>;
}

/// Sends events with partition keys.
///
/// This trait is part of the [`Transport`] trait hierarchy and provides
/// event-specific sending capabilities.
pub trait EventSender {
    /// Sends an event message with the given key.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be sent.
    fn send_event(&self, key: &[u8], message: &[u8]) -> Result<()>;
}

/// Unified transport abstraction for CQRS messaging.
///
/// The Transport trait combines command response and event sending capabilities
/// with async message consumption. Implementations typically wrap a message broker
/// like Kafka or an in-memory channel.
#[async_trait]
pub trait Transport: Send + Sync + EventSender + Sized {
    /// Sends a command response message.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be sent.
    fn send_command_response(&self, key: &[u8], message: &[u8]) -> Result<()>;

    /// Consumes messages asynchronously in a blocking manner.
    ///
    /// This method typically runs in a loop, continuously processing messages
    /// from the underlying transport and passing them to the command service server.
    ///
    /// `E` is the closed event enum the paired [`CommandServiceServer`] produces.
    /// `Transport` is never used as a trait object (it requires `Sized`), so a
    /// generic method here doesn't affect object safety.
    ///
    /// # Errors
    ///
    /// Returns an error if message consumption fails.
    async fn consume_async_blocking<E: EventEnvelope + Send + Sync + 'static>(
        &self,
        command_service_server: CommandServiceServer<Self, E>,
    ) -> Result<()>;
}

/// Consumes messages from an inbound channel synchronously.
///
/// This trait is provided for simpler synchronous use cases.
pub trait InboundChannel {
    /// Attempts to consume a message from the channel.
    ///
    /// Returns `None` if no message is available.
    fn consume(&self) -> Option<Vec<u8>>;
}

/// Consumes messages from a stream asynchronously.
///
/// This trait is deprecated in favor of the unified [`Transport`] trait.
pub trait StreamInboundChannel {
    /// Consumes messages asynchronously in a blocking manner.
    ///
    /// # Errors
    ///
    /// Returns an error if consumption fails.
    fn consume_async_blocking(&self) -> Result<()>;
}

/// Consumes and processes messages asynchronously.
///
/// Implemented by both [`CommandServiceClient`] for processing command responses
/// and [`EventListener`] for processing events.
///
/// [`CommandServiceClient`]: crate::cqrs::CommandServiceClient
/// [`EventListener`]: crate::cqrs::EventListener
#[async_trait]
pub trait MessageConsumer {
    /// Processes a single message.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be processed.
    async fn consume(&self, message: &[u8]) -> Result<()>;
}

/// Represents a command in the CQRS system.
///
/// Commands are requests to perform an action that will result in state changes.
/// A client always sends one concrete command type at a time, so this trait is
/// used generically (`<C: Command>`), never as a trait object. Bound by
/// `DeserializeOwned` rather than a borrowed `Deserialize<'de>` — no command
/// struct in practice borrows from the input buffer (they're all owned
/// `String` fields), so there's no zero-copy benefit to threading a lifetime
/// through every API that touches `Command`.
///
/// # Examples
///
/// ```ignore
/// use serde::{Deserialize, Serialize};
/// use cqrs_library::cqrs::traits::Command;
///
/// #[derive(Deserialize, Serialize)]
/// struct CreateUserCommand {
///     user_id: String,
///     name: String,
/// }
///
/// impl Command for CreateUserCommand {
///     fn get_subject(&self) -> String {
///         self.user_id.clone()
///     }
///
///     fn get_type(&self) -> String {
///         "CreateUserCommand".to_string()
///     }
/// }
/// ```
pub trait Command: DeserializeOwned + Serialize {
    /// Returns the subject (aggregate ID) this command operates on.
    ///
    /// This is typically the ID of the entity being modified.
    fn get_subject(&self) -> String;

    /// Returns the command type identifier.
    ///
    /// This should be a unique string identifying the command type. On the
    /// wire, a service's [`CommandEnvelope`] enum variant name must match
    /// this value so the server can deserialize directly into it.
    fn get_type(&self) -> String;

    /// Returns the version of this command schema.
    ///
    /// Defaults to 1. Override to support command schema evolution.
    fn get_version(&self) -> i32 {
        1
    }
}
