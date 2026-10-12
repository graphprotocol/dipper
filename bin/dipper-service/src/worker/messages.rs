use super::handlers::{
    CancelRejectedAgreementOnChain, ReassessIndexingRequest, SendIndexingAgreementProposal,
    SubmitOffer,
};

/// The queue worker message enum
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    /// Reassess an indexing request against the current IISA target state.
    ///
    /// The admin RPC queues this on every `set_indexing_target_candidates` call
    /// that mutates a request row. Reassessment diffs the IISA target group of
    /// size `num_candidates` against the current active agreements, then
    /// proposes additions and fires on-chain cancels for removals.
    ///
    /// See [`ReassessIndexingRequest`] for more details.
    ReassessIndexingRequest(ReassessIndexingRequest),

    /// Send an indexing agreement proposal to the indexer.
    ///
    /// See [`SendIndexingAgreementProposal`] for more details.
    SendIndexingAgreementProposal(SendIndexingAgreementProposal),

    /// Check on-chain an agreement dipper rejected or cancelled that the chain listener saw
    /// accepted. See [`CancelRejectedAgreementOnChain`] for more details.
    CancelRejectedAgreementOnChain(CancelRejectedAgreementOnChain),

    /// Submit an RCA offer on-chain after the indexer has accepted the
    /// proposal terms.
    ///
    /// Runs after `send_indexing_agreement_proposal` receives an Accept
    /// response: posts the RCA via `RecurringCollector.offer()` so the
    /// contract has the offer hash when the indexer-agent later calls
    /// `acceptIndexingAgreement` on-chain.
    ///
    /// See [`SubmitOffer`] for more details.
    SubmitOffer(SubmitOffer),
}
