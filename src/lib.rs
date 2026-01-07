use opentelemetry_proto::{
    tonic::collector::logs::v1::{
        ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
    },
    transform::{
        common::tonic::ResourceAttributesWithSchema, logs::tonic::group_logs_by_resource_and_scope,
    },
};

pub mod error;
use error::Error;
pub use opentelemetry_proto;

pub mod retry;
use opentelemetry_sdk::{
    Resource,
    error::{OTelSdkError, OTelSdkResult},
    logs::{LogBatch, LogExporter},
};
use retry::RetryPolicy;
use tokio::sync::Mutex;
use tonic::{codec::CompressionEncoding, transport::Channel};

use crate::retry::export_with_retry;

/// OtlpLogsExporter is a log exporter for OpenTelemetry that uses Tonic to send
/// logs to a collector.
///
/// It allows you to specify a retry policy for retrying failed requests based
/// on whether the Otel errors are retryable or not.
#[derive(Debug)]
pub struct OtlpLogsExporter {
    client: Mutex<LogsServiceClient<Channel>>,
    retry_policy: RetryPolicy,
    resource: ResourceAttributesWithSchema,
}

impl OtlpLogsExporter {
    pub async fn with_default_retry(endpoint: &str) -> Result<Self, Error> {
        let retry_policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1600,
            jitter_ms: 100,
        };

        Self::new(endpoint, retry_policy).await
    }

    pub fn with_channel(channel: Channel, retry_policy: RetryPolicy) -> Self {
        let client = LogsServiceClient::new(channel)
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd);

        Self {
            retry_policy,
            client: Mutex::new(client),
            resource: Default::default(),
        }
    }

    pub async fn new(endpoint: &str, retry_policy: RetryPolicy) -> Result<Self, Error> {
        let channel_builder = Channel::from_shared(endpoint.to_string())?;
        let channel = channel_builder.connect().await?;
        Ok(Self::with_channel(channel, retry_policy))
    }

    /// Export a single logs request.
    ///
    /// This function will retry if the request fails based on the exporter's
    /// retry policy.
    pub async fn send_request(&mut self, request: ExportLogsServiceRequest) -> Result<(), Error> {
        let mut client = self.client.lock().await;
        export_with_retry(&mut client, &self.retry_policy, &request).await
    }
}

impl LogExporter for OtlpLogsExporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let resource_logs = group_logs_by_resource_and_scope(batch, &self.resource);
        let request = ExportLogsServiceRequest { resource_logs };

        let mut client = self.client.lock().await;

        match export_with_retry(&mut client, &self.retry_policy, &request).await {
            Ok(_) => Ok(()),
            Err(error) => Err(OTelSdkError::InternalFailure(format!(
                "OTLP export error: {error:?}"
            ))),
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }
}

#[cfg(test)]
mod tests {
    use crate::retry::{RetryErrorType, classify_tonic_status};
    use tonic::Status;

    #[test]
    fn test_classify_unavailable_error() {
        let status = Status::unavailable("Service unavailable");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::Retryable);
    }

    #[test]
    fn test_classify_invalid_argument_error() {
        let status = Status::invalid_argument("Bad request");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }

    #[test]
    fn test_classify_deadline_exceeded_error() {
        let status = Status::deadline_exceeded("Timeout");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::Retryable);
    }

    #[test]
    fn test_classify_cancelled_error() {
        let status = Status::cancelled("Request cancelled");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::Retryable);
    }

    #[test]
    fn test_classify_permission_denied_error() {
        let status = Status::permission_denied("Not authorized");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }

    #[test]
    fn test_classify_unauthenticated_error() {
        let status = Status::unauthenticated("Authentication failed");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }

    #[test]
    fn test_classify_internal_error() {
        let status = Status::internal("Internal server error");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }

    #[test]
    fn test_classify_resource_exhausted_without_retry_info() {
        let status = Status::resource_exhausted("Too many requests");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }

    #[test]
    fn test_classify_not_found_error() {
        let status = Status::not_found("Resource not found");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }

    #[test]
    fn test_classify_already_exists_error() {
        let status = Status::already_exists("Resource already exists");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }

    #[test]
    fn test_classify_failed_precondition_error() {
        let status = Status::failed_precondition("Precondition failed");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }

    #[test]
    fn test_classify_out_of_range_error() {
        let status = Status::out_of_range("Value out of range");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::Retryable);
    }

    #[test]
    fn test_classify_unimplemented_error() {
        let status = Status::unimplemented("Feature not implemented");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }

    #[test]
    fn test_classify_data_loss_error() {
        let status = Status::data_loss("Data loss occurred");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::Retryable);
    }

    #[test]
    fn test_classify_unknown_error() {
        let status = Status::unknown("Unknown error");
        let classification = classify_tonic_status(&status);
        assert_eq!(classification, RetryErrorType::NonRetryable);
    }
}
