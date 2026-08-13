use clap::Parser;
use colored::Colorize;
use config::Config;
use cqrs_kafka::inbound::StreamKafkaInboundChannel;
use cqrs_kafka::outbound::KafkaOutboundChannel;
use cqrs_library::cqrs::messages::CommandResponse;
use cqrs_library::cqrs::traits::Command;
use cqrs_library::cqrs::{CommandServiceClient, EventListener};
use hdrhistogram::Histogram;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, io};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// CQRS Kafka Performance Testing Tool
///
/// Measures end-to-end latency for commands sent through Kafka.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of commands to send
    #[arg(short, long, default_value_t = 1000)]
    count: usize,

    /// Number of concurrent workers
    #[arg(short = 'w', long, default_value_t = 10)]
    workers: usize,

    /// Number of warmup requests (not counted in statistics)
    #[arg(long, default_value_t = 100)]
    warmup: usize,

    /// Bootstrap server (overrides config)
    #[arg(short, long)]
    bootstrap_server: Option<String>,

    /// Command topic (overrides config)
    #[arg(short = 't', long)]
    command_topic: Option<String>,

    /// Enable detailed output
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateUserCommand {
    user_id: String,
    name: String,
}

impl Command for CreateUserCommand {
    fn get_subject(&self) -> String {
        self.user_id.to_owned()
    }
    fn get_type(&self) -> String {
        String::from("CreateUserCommand")
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct UserCreatedEvent {
    user_id: String,
    name: String,
}

/// Closes over every event type this service listens for.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum DomainEvent {
    UserCreatedEvent(UserCreatedEvent),
}

fn handle_event(event: DomainEvent) {
    match event {
        DomainEvent::UserCreatedEvent(event) => {
            info!("Received event {:?}", event);
        }
    }
}

/// Represents the result of a single command execution
struct CommandResult {
    latency_micros: u64,
    success: bool,
}

/// Performance statistics
struct Stats {
    histogram: Histogram<u64>,
    total_requests: usize,
    successful_requests: usize,
    failed_requests: usize,
    total_duration: Duration,
}

impl Stats {
    fn new() -> Self {
        Stats {
            histogram: Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            total_duration: Duration::ZERO,
        }
    }

    fn record(&mut self, result: CommandResult) {
        self.total_requests += 1;
        if result.success {
            self.successful_requests += 1;
            let _ = self.histogram.record(result.latency_micros);
        } else {
            self.failed_requests += 1;
        }
    }

    fn print_report(&self) {
        println!("\n{}", "═══════════════════════════════════════════════════".bold());
        println!("{}", "         KAFKA CQRS LATENCY TEST RESULTS".bold().cyan());
        println!("{}", "═══════════════════════════════════════════════════".bold());

        println!("\n{}", "Summary:".bold());
        println!("  Total Requests:      {}", self.total_requests);
        println!("  Successful:          {} {}",
            self.successful_requests,
            format!("({:.1}%)", (self.successful_requests as f64 / self.total_requests as f64) * 100.0).green()
        );
        if self.failed_requests > 0 {
            println!("  Failed:              {} {}",
                self.failed_requests,
                format!("({:.1}%)", (self.failed_requests as f64 / self.total_requests as f64) * 100.0).red()
            );
        } else {
            println!("  Failed:              {}", self.failed_requests);
        }
        println!("  Total Duration:      {:.2}s", self.total_duration.as_secs_f64());
        println!("  Throughput:          {:.2} req/s",
            self.successful_requests as f64 / self.total_duration.as_secs_f64()
        );

        if self.successful_requests > 0 {
            println!("\n{}", "Latency Distribution (milliseconds):".bold());
            println!("  Min:                 {:.2} ms", self.histogram.min() as f64 / 1000.0);
            println!("  Mean:                {:.2} ms", self.histogram.mean() / 1000.0);
            println!("  Max:                 {:.2} ms", self.histogram.max() as f64 / 1000.0);
            println!("  Std Dev:             {:.2} ms", self.histogram.stdev() / 1000.0);

            println!("\n{}", "Percentiles:".bold());
            println!("  p50 (median):        {:.2} ms", self.histogram.value_at_percentile(50.0) as f64 / 1000.0);
            println!("  p75:                 {:.2} ms", self.histogram.value_at_percentile(75.0) as f64 / 1000.0);
            println!("  p90:                 {:.2} ms", self.histogram.value_at_percentile(90.0) as f64 / 1000.0);
            println!("  p95:                 {:.2} ms {}",
                self.histogram.value_at_percentile(95.0) as f64 / 1000.0,
                if self.histogram.value_at_percentile(95.0) < 100_000 { "✓".green() } else { "!".red() }
            );
            println!("  p99:                 {:.2} ms {}",
                self.histogram.value_at_percentile(99.0) as f64 / 1000.0,
                if self.histogram.value_at_percentile(99.0) < 200_000 { "✓".green() } else { "!".red() }
            );
            println!("  p99.9:               {:.2} ms", self.histogram.value_at_percentile(99.9) as f64 / 1000.0);
            println!("  p99.99:              {:.2} ms", self.histogram.value_at_percentile(99.99) as f64 / 1000.0);
        }

        println!("\n{}", "═══════════════════════════════════════════════════".bold());
    }
}

async fn send_command_and_measure(
    client: &CommandServiceClient<KafkaOutboundChannel>,
    command_num: usize,
) -> CommandResult {
    let command = CreateUserCommand {
        user_id: Uuid::new_v4().to_string(),
        name: format!("User {}", command_num),
    };

    let start = Instant::now();
    let result = client.send_command(&command).await;
    let latency = start.elapsed();

    match result {
        Ok(CommandResponse::Ok) => CommandResult {
            latency_micros: latency.as_micros() as u64,
            success: true,
        },
        Ok(response) => {
            warn!("Command {} returned non-OK response: {:?}", command_num, response);
            CommandResult {
                latency_micros: latency.as_micros() as u64,
                success: false,
            }
        }
        Err(e) => {
            warn!("Command {} failed: {:?}", command_num, e);
            CommandResult {
                latency_micros: latency.as_micros() as u64,
                success: false,
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    // Load configuration
    let settings = Arc::new(
        Config::builder()
            .add_source(config::File::with_name("cqrs-performance/Settings").required(false))
            .add_source(config::File::with_name("Settings").required(false))
            .build()
            .unwrap_or_else(|_| Config::builder().build().unwrap()),
    );

    // Setup logging
    if env::var("RUST_LOG").is_err() {
        let log_level = if args.verbose { "debug" } else { "info" };
        env::set_var("RUST_LOG", log_level);
    }
    env_logger::init();

    println!("{}", "═══════════════════════════════════════════════════".bold());
    println!("{}", "      CQRS Kafka Performance Testing Tool".bold().cyan());
    println!("{}", "═══════════════════════════════════════════════════".bold());
    println!("\n{}", "Configuration:".bold());
    println!("  Commands:            {}", args.count);
    println!("  Workers:             {}", args.workers);
    println!("  Warmup:              {}", args.warmup);

    // Get configuration values
    let bootstrap_server = args
        .bootstrap_server
        .or_else(|| settings.get_string("bootstrap_server").ok())
        .expect("Bootstrap server not specified (use --bootstrap-server or config file)");

    let command_topic = args
        .command_topic
        .or_else(|| settings.get_string("command_topic").ok())
        .unwrap_or_else(|| "commands".to_string());

    let service_id = settings
        .get_string("service_id")
        .unwrap_or_else(|_| "perf-test-client".to_string());

    println!("  Bootstrap Server:    {}", bootstrap_server);
    println!("  Command Topic:       {}", command_topic);
    println!("  Service ID:          {}", service_id);

    // Optional: Start event listener in background
    if let Ok(subscriptions) = settings.get_string("service_subscriptions") {
        let settings_clone = settings.clone();
        tokio::spawn(async move {
            let event_listener = EventListener::new(handle_event);

            let topics: Vec<&str> = subscriptions.split(',').collect();
            let transaction_handler = cqrs_kafka::traits::NoopTransactionHandler::default();

            if let Ok(channel) = StreamKafkaInboundChannel::new(
                &settings_clone.get_string("service_id").unwrap(),
                &topics,
                &settings_clone.get_string("bootstrap_server").unwrap(),
                Arc::new(event_listener),
                &transaction_handler,
                false,
            ) {
                let _ = channel.consume_async_blocking().await;
            }
        });
    }

    // Create command client
    let kafka_command_channel =
        KafkaOutboundChannel::new(command_topic.clone(), &bootstrap_server)
            .expect("Could not create kafka command channel");

    let client = Arc::new(CommandServiceClient::new(&service_id, kafka_command_channel));

    // Listen for command responses and feed them back into the client, otherwise
    // every `send_command` call would block forever waiting for a reply.
    let command_response_topic = settings
        .get_string("command_response_topic")
        .unwrap_or_else(|_| "command-responses".to_string());
    println!("  Response Topic:      {}", command_response_topic);

    let response_client = Arc::clone(&client);
    let response_bootstrap_server = bootstrap_server.clone();
    let response_service_id = service_id.clone();
    tokio::spawn(async move {
        let transaction_handler = cqrs_kafka::traits::NoopTransactionHandler::default();
        let channel = StreamKafkaInboundChannel::new(
            &format!("{}-RESPONSES", response_service_id),
            &[command_response_topic.as_str()],
            &response_bootstrap_server,
            response_client,
            &transaction_handler,
            false,
        )
        .expect("Could not create command response channel");

        channel
            .consume_async_blocking()
            .await
            .expect("Failed to consume command response channel");
    });

    // Give the response consumer's group rebalance time to complete before
    // sending any commands. Otherwise, since it resets to the latest offset,
    // responses produced before the rebalance finishes would never be seen,
    // leaving `send_command` waiting forever.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Warmup phase
    if args.warmup > 0 {
        println!("\n{}", "Running warmup...".yellow());
        let warmup_handles: Vec<JoinHandle<CommandResult>> = (0..args.warmup)
            .map(|i| {
                let client = client.clone();
                tokio::spawn(async move { send_command_and_measure(&client, i).await })
            })
            .collect();

        for handle in warmup_handles {
            let _ = handle.await;
        }
        println!("{}", "Warmup complete!".green());
    }

    // Main test phase
    println!("\n{}", "Starting performance test...".yellow());
    let test_start = Instant::now();
    let mut stats = Stats::new();

    // Process commands in batches based on worker count
    let batch_size = args.workers;
    let total_batches = (args.count + batch_size - 1) / batch_size;

    for batch in 0..total_batches {
        let batch_start = batch * batch_size;
        let batch_end = ((batch + 1) * batch_size).min(args.count);

        let handles: Vec<JoinHandle<CommandResult>> = (batch_start..batch_end)
            .map(|i| {
                let client = client.clone();
                tokio::spawn(async move { send_command_and_measure(&client, i).await })
            })
            .collect();

        for handle in handles {
            if let Ok(result) = handle.await {
                stats.record(result);
            }
        }

        if args.verbose && batch % 10 == 0 {
            println!(
                "Progress: {}/{} ({:.1}%)",
                batch_end,
                args.count,
                (batch_end as f64 / args.count as f64) * 100.0
            );
        }
    }

    stats.total_duration = test_start.elapsed();

    // Print results
    stats.print_report();

    Ok(())
}
