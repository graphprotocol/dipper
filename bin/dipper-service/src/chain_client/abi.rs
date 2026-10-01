//! Contract ABI definitions for on-chain interactions.

use thegraph_core::alloy::{sol, sol_types::SolInterface};

sol! {
    /// RecurringCollector contract interface (the EIP-712 domain read dipper
    /// needs). Full contract: `RecurringCollector.sol` in `graphprotocol/contracts`.
    #[allow(missing_docs)]
    #[derive(Debug)]
    interface IRecurringCollector {
        /// Agreement details returned from `offer()`. `state` is a bitmask of the
        /// flags in `IAgreementCollector.sol` (REGISTERED=1 through UPDATE=128;
        /// `uint16` as values exceed `uint8`). Undecoded; the layout must match.
        struct AgreementDetails {
            bytes16 agreementId;
            address payer;
            address dataService;
            address serviceProvider;
            bytes32 versionHash;
            uint16 state;
        }

        /// Store a new or updated RCA offer on-chain. `offerType`: 1 = OFFER_TYPE_NEW,
        /// 2 = OFFER_TYPE_UPDATE (0 is a reserved sentinel that reverts). `data` is the
        /// ABI-encoded `RecurringCollectionAgreement`; `options` is reserved, pass 0.
        function offer(uint8 offerType, bytes calldata data, uint16 options)
            external
            returns (AgreementDetails memory details);

        /// Read-only details for the agreement at a version index. Index 0 is
        /// VERSION_CURRENT (the active or pre-acceptance terms); the returned
        /// `state` bitmask says whether the agreement is still live on-chain.
        function getAgreementDetails(bytes16 agreementId, uint256 index)
            external
            view
            returns (AgreementDetails memory);

        /// Emitted when `offer()` stores a new or updated RCA offer.
        event OfferStored(
            bytes16 indexed agreementId,
            address indexed payer,
            uint8 indexed offerType,
            bytes32 offerHash
        );

        /// EIP-5267 report of the EIP-712 domain the contract verifies
        /// signatures under. Inherited from OpenZeppelin's EIP712Upgradeable;
        /// dipper fetches it at startup (see `chain_client::eip5267`).
        function eip712Domain()
            external
            view
            returns (
                bytes1 fields,
                string memory name,
                string memory version,
                uint256 chainId,
                address verifyingContract,
                bytes32 salt,
                uint256[] memory extensions
            );

        // Custom errors copied from IRecurringCollector.sol so revert payloads
        // decode to names. Solidity enum params are declared uint8 here: the
        // ABI canonicalizes enums to uint8, so computed selectors still match.
        error RecurringCollectorAgreementIdZero();
        error RecurringCollectorDataServiceNotAuthorized(bytes16 agreementId, address unauthorizedDataService);
        error RecurringCollectorUnauthorizedDataService(address dataService);
        error RecurringCollectorAgreementDeadlineElapsed(uint256 currentTimestamp, uint64 deadline);
        error RecurringCollectorInvalidSigner();
        error RecurringCollectorUnauthorizedCaller(address unauthorizedCaller, address dataService);
        error RecurringCollectorInvalidCollectData(bytes invalidData);
        error RecurringCollectorInvalidOfferType(uint8 offerType);
        error RecurringCollectorAgreementIncorrectState(bytes16 agreementId, uint8 incorrectState);
        error RecurringCollectorAgreementNotCollectable(bytes16 agreementId, uint8 reason);
        error RecurringCollectorAgreementAddressNotSet();
        error RecurringCollectorAgreementEndsBeforeDeadline(uint64 deadline, uint64 endsAt);
        error RecurringCollectorAgreementInvalidCollectionWindow(
            uint32 allowedMinCollectionWindow,
            uint32 minSecondsPerCollection,
            uint32 maxSecondsPerCollection
        );
        error RecurringCollectorAgreementInvalidDuration(uint32 requiredMinDuration, uint256 invalidDuration);
        error RecurringCollectorCollectionTooSoon(bytes16 agreementId, uint32 secondsSinceLast, uint32 minSeconds);
        error RecurringCollectorInvalidUpdateNonce(bytes16 agreementId, uint32 expected, uint32 provided);
        error RecurringCollectorExcessiveSlippage(uint256 requested, uint256 actual, uint256 maxSlippage);
        error RecurringCollectorCollectionNotEligible(bytes16 agreementId, address serviceProvider);
        error RecurringCollectorPayerDoesNotSupportInterface(address payer, bytes4 interfaceId);
        error RecurringCollectorInsufficientCallbackGas();
        error RecurringCollectorNotGovernor(address account);
        error RecurringCollectorNotPauseGuardian(address account);
        error RecurringCollectorPauseGuardianNoChange(address account, bool allowed);
        error RecurringCollectorOfferCancelled(address signer, bytes32 hash);
    }

    /// RecurringAgreementManager (RAM) contract interface (minimal subset). In
    /// protocol-managed mode the manager is the RCA payer; dipper drives it as an
    /// unsigned operator while the manager funds escrow.
    #[allow(missing_docs)]
    #[derive(Debug)]
    interface IRecurringAgreementManager {
        /// Offer an agreement through the manager. `offerType` mirrors
        /// `RecurringCollector` (1 = OFFER_TYPE_NEW); `offerData` is the
        /// ABI-encoded `RecurringCollectionAgreement` whose payer is this manager.
        function offerAgreement(address collector, uint8 offerType, bytes calldata offerData)
            external
            returns (bytes16 agreementId);

        /// Cancel an agreement through the manager. `versionHash` is the EIP-712
        /// terms hash the collector stored; `options` selects the cancel scope
        /// (1 = active, 2 = pending).
        function cancelAgreement(address collector, bytes16 agreementId, bytes32 versionHash, uint16 options)
            external;

        /// Rebalance a provider's escrow across all its agreements, draining and
        /// cleaning up ended or canceled ones. Permissionless and idempotent;
        /// returns whether the provider is still tracked afterwards.
        function reconcileProvider(address collector, address provider)
            external
            returns (bool tracked);

        /// Reconcile a single agreement's escrow through the manager.
        /// Permissionless and idempotent; returns whether the agreement is
        /// still tracked afterwards.
        function reconcileAgreement(address collector, bytes16 agreementId)
            external
            returns (bool tracked);

        /// The manager's escrow account with one provider, as PaymentsEscrow holds it.
        struct EscrowAccount {
            uint256 balance;
            uint256 tokensThawing;
            uint256 thawEndTimestamp;
        }

        /// Number of providers the manager tracks escrow for under `collector`.
        function getProviderCount(address collector) external view returns (uint256);

        /// The provider at `index` in the manager's tracked set for `collector`.
        function getProviderAt(address collector, uint256 index) external view returns (address);

        /// The manager's escrow account with `provider` under `collector`.
        function getEscrowAccount(address collector, address provider)
            external
            view
            returns (EscrowAccount memory account);

        /// Emitted when the manager stores a new agreement.
        event AgreementAdded(
            bytes16 indexed agreementId,
            address indexed collector,
            address indexed dataService,
            address provider
        );

        /// Emitted when the manager cancels an agreement.
        event AgreementRemoved(bytes16 indexed agreementId);

        // Custom errors copied from IRecurringAgreementManagement.sol and the
        // RecurringAgreementManager contract, plus the OpenZeppelin Pausable and
        // AccessControl errors its BaseUpgradeable inherits.
        error AgreementIdZero();
        error ServiceProviderZeroAddress();
        error UnauthorizedDataService(address dataService);
        error UnauthorizedCollector(address collector);
        error PayerMismatch(address payer);
        error InvalidIssuanceAllocator(address allocator);
        error CannotRevokeGovernorRole();
        error EnforcedPause();
        error ExpectedPause();
        error AccessControlUnauthorizedAccount(address account, bytes32 neededRole);
        error AccessControlBadConfirmation();
    }
}

/// Render a revert payload as the RecurringCollector or RecurringAgreementManager
/// error it decodes to, or the raw 4-byte selector for a payload dipper does not
/// recognize.
pub(crate) fn decode_revert_reason(selector: [u8; 4], data: &[u8]) -> String {
    if let Ok(err) = IRecurringCollector::IRecurringCollectorErrors::abi_decode(data) {
        return format!("{err:?}");
    }
    if let Ok(err) = IRecurringAgreementManager::IRecurringAgreementManagerErrors::abi_decode(data)
    {
        return format!("{err:?}");
    }
    format!(
        "unrecognized revert selector 0x{:02x}{:02x}{:02x}{:02x}",
        selector[0], selector[1], selector[2], selector[3]
    )
}

#[cfg(test)]
mod tests {
    use thegraph_core::alloy::sol_types::SolError;

    use super::*;

    #[test]
    fn decodes_the_observed_collection_window_revert() {
        //* Arrange - the revert produced by the 60/240 misconfiguration
        let err = IRecurringCollector::RecurringCollectorAgreementInvalidCollectionWindow {
            allowedMinCollectionWindow: 600,
            minSecondsPerCollection: 60,
            maxSecondsPerCollection: 240,
        };
        let data = err.abi_encode();
        let mut selector = [0u8; 4];
        selector.copy_from_slice(&data[..4]);

        //* Act
        let reason = decode_revert_reason(selector, &data);

        //* Assert - selector pinned to the value observed on-chain
        assert_eq!(
            selector,
            [0xe4, 0x57, 0x63, 0x96],
            "declared error signature drifted from the contract"
        );
        assert!(
            reason.contains("RecurringCollectorAgreementInvalidCollectionWindow"),
            "reason should name the error: {reason}"
        );
        assert!(
            reason.contains("240"),
            "reason should carry the field values: {reason}"
        );
    }

    #[test]
    fn decodes_a_manager_side_revert() {
        //* Arrange - the manager rejects a collector without COLLECTOR_ROLE
        let err = IRecurringAgreementManager::UnauthorizedCollector {
            collector: thegraph_core::alloy::primitives::Address::ZERO,
        };
        let data = err.abi_encode();
        let mut selector = [0u8; 4];
        selector.copy_from_slice(&data[..4]);

        //* Act
        let reason = decode_revert_reason(selector, &data);

        //* Assert
        assert!(
            reason.contains("UnauthorizedCollector"),
            "reason should name the manager error: {reason}"
        );
    }

    #[test]
    fn falls_back_to_the_raw_selector_for_unknown_errors() {
        //* Arrange - a selector no declared error matches
        let data = [0xde, 0xad, 0xbe, 0xef, 0x00, 0x00];

        //* Act
        let reason = decode_revert_reason([0xde, 0xad, 0xbe, 0xef], &data);

        //* Assert
        assert!(reason.contains("0xdeadbeef"), "reason: {reason}");
    }
}
