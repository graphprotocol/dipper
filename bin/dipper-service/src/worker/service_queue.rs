use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use super::{
    handlers::{ReassessIndexingRequest, SendIndexingAgreementProposal, SubmitOffer},
    messages::Message,
    queue::{JobId, JobPriority, Queue},
};

#[async_trait]
pub trait WorkerQueue {
    async fn send_indexing_agreement_proposal(
        &self,
        candidate_url: Url,
        agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        priority: JobPriority,
    ) -> anyhow::Result<JobId>;

    async fn reassess_indexing_request(
        &self,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: usize,
        priority: JobPriority,
    ) -> anyhow::Result<JobId>;

    /// Submit an RCA offer on-chain as the first step of a new proposal. The
    /// job retries until the indexer's window to accept has closed, so a
    /// provider outage costs one offer only if it outlasts that window.
    async fn submit_offer(
        &self,
        agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        indexer_url: Url,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        priority: JobPriority,
    ) -> anyhow::Result<JobId>;
}

/// The worker that processes messages from the queue.
#[derive(Clone)]
pub struct WorkerQueueHandle<Q> {
    queue: Q,
    /// Retries allowed on an offer submission, sized so the budget lasts as
    /// long as the indexer's window to accept. See [`WorkerQueue::submit_offer`].
    submit_offer_max_retries: u32,
}

impl<Q> WorkerQueueHandle<Q> {
    /// Create a new instance of the worker queue handle
    pub(super) fn new(queue: Q, submit_offer_max_retries: u32) -> Self {
        Self {
            queue,
            submit_offer_max_retries,
        }
    }
}

#[async_trait]
impl<Q> WorkerQueue for WorkerQueueHandle<Q>
where
    Q: Queue<Message> + Send + Sync,
{
    async fn send_indexing_agreement_proposal(
        &self,
        indexer_url: Url,
        agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        priority: JobPriority,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(
                Message::SendIndexingAgreementProposal(SendIndexingAgreementProposal {
                    indexer_url,
                    agreement_id,
                    indexing_request_id,
                    deployment_id,
                    deployment_chain_id,
                }),
                priority,
            )
            .await
    }

    async fn reassess_indexing_request(
        &self,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: usize,
        priority: JobPriority,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(
                Message::ReassessIndexingRequest(ReassessIndexingRequest {
                    indexing_request_id,
                    deployment_id,
                    deployment_chain_id,
                    num_candidates,
                }),
                priority,
            )
            .await
    }

    async fn submit_offer(
        &self,
        agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        indexer_url: Url,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        priority: JobPriority,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push_with_max_retries(
                Message::SubmitOffer(SubmitOffer {
                    agreement_id,
                    indexing_request_id,
                    indexer_url,
                    deployment_id,
                    deployment_chain_id,
                }),
                priority,
                self.submit_offer_max_retries,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use thegraph_core::deployment_id;

    use super::*;
    use crate::worker::queue::{JobGuard, JobNotifications};

    /// Records the retry budget each push carried, `None` for a push that took
    /// the queue-wide default.
    #[derive(Default)]
    struct RecordingQueue {
        pushes: Mutex<Vec<Option<u32>>>,
    }

    struct NeverNotifies;

    #[async_trait]
    impl JobNotifications for NeverNotifies {
        async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("these tests never subscribe")
        }
    }

    #[async_trait]
    impl Queue<Message> for RecordingQueue {
        type Listener = NeverNotifies;

        async fn push(&self, _msg: Message, _priority: JobPriority) -> anyhow::Result<JobId> {
            self.pushes.lock().unwrap().push(None);
            Ok(JobId::default())
        }

        async fn push_with_max_retries(
            &self,
            _msg: Message,
            _priority: JobPriority,
            max_retries: u32,
        ) -> anyhow::Result<JobId> {
            self.pushes.lock().unwrap().push(Some(max_retries));
            Ok(JobId::default())
        }

        async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, Message>>> {
            Ok(None)
        }

        async fn subscribe(&self) -> anyhow::Result<Self::Listener> {
            anyhow::bail!("these tests never subscribe")
        }
    }

    fn handle(max_retries: u32) -> WorkerQueueHandle<RecordingQueue> {
        WorkerQueueHandle::new(RecordingQueue::default(), max_retries)
    }

    /// The budget the worker sized from the acceptance window has to reach the
    /// job. Falling back to the queue default is the bug this guards: 3 attempts
    /// inside 95 seconds, then nothing for the rest of a 600 second window.
    #[tokio::test]
    async fn an_offer_submission_carries_the_window_sized_retry_budget() {
        //* Arrange
        let queue = handle(4);

        //* Act
        queue
            .submit_offer(
                IndexingAgreementId::from_bytes([0; 16]),
                IndexingRequestId::new(),
                "https://indexer.example.com".parse().unwrap(),
                deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
                1,
                JobPriority::Background,
            )
            .await
            .unwrap();

        //* Assert
        assert_eq!(*queue.queue.pushes.lock().unwrap(), vec![Some(4)]);
    }

    /// A proposal has no deadline of its own to spend retries against, so it
    /// keeps the queue-wide budget.
    #[tokio::test]
    async fn a_proposal_keeps_the_queue_default_retry_budget() {
        //* Arrange
        let queue = handle(4);

        //* Act
        queue
            .send_indexing_agreement_proposal(
                "https://indexer.example.com".parse().unwrap(),
                IndexingAgreementId::from_bytes([0; 16]),
                IndexingRequestId::new(),
                deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
                1,
                JobPriority::Background,
            )
            .await
            .unwrap();

        //* Assert
        assert_eq!(*queue.queue.pushes.lock().unwrap(), vec![None]);
    }
}
