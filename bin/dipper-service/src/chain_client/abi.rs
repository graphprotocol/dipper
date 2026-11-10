//! Contract ABI definitions for on-chain interactions.
//!
//! These are written by hand, as the subset of each contract dipper uses. The tests below
//! compare every declaration with `contract_interfaces.txt`, a snapshot of the contracts' own
//! interfaces written by `scripts/snapshot-contract-interfaces.py`, so one that has drifted from
//! the contracts fails the build. After the contracts change, write the snapshot again.

use thegraph_core::alloy::{sol, sol_types::SolInterface};

sol! {
    /// RecurringCollector contract interface (the EIP-712 domain read dipper
    /// needs). Full contract: `RecurringCollector.sol` in `graphprotocol/contracts`.
    #[allow(missing_docs)]
    #[derive(Debug)]
    #[sol(abi)]
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

        /// The most the payer could still owe on the agreement; 0 once it has ended
        /// and nothing more can be collected.
        function getMaxNextClaim(bytes16 agreementId) external view returns (uint256);

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
    #[sol(abi)]
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
        function getProviderCount(address collector) external view returns (uint256 count);

        /// The provider at `index` in the manager's tracked set for `collector`.
        function getProviderAt(address collector, uint256 index)
            external
            view
            returns (address provider);

        /// The manager's escrow account with `provider` under `collector`.
        function getEscrowAccount(address collector, address provider)
            external
            view
            returns (EscrowAccount memory account);

        /// Number of agreements the manager tracks for `provider` under `collector`.
        function getAgreementCount(address collector, address provider)
            external
            view
            returns (uint256 count);

        /// The agreement at `index` among those the manager tracks for `provider`.
        function getAgreementAt(address collector, address provider, uint256 index)
            external
            view
            returns (bytes16 agreementId);

        /// Emitted when the manager stores a new agreement.
        event AgreementAdded(
            bytes16 indexed agreementId,
            address indexed collector,
            address dataService,
            address indexed provider
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
    use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
    use thegraph_core::alloy::{
        json_abi::{JsonAbi, Param, StateMutability},
        sol_types::{SolError, SolStruct},
    };

    use super::{super::manager_role::IAccessControl, *};

    /// The contracts' own interfaces, written by `scripts/snapshot-contract-interfaces.py`.
    const CONTRACT_INTERFACES: &str = include_str!("contract_interfaces.txt");

    /// The lines the snapshot lists for `contract`.
    fn in_the_contracts(contract: &str) -> Vec<&'static str> {
        let (_, section) = CONTRACT_INTERFACES
            .split_once(&format!("[{contract}]\n"))
            .expect("the snapshot has a section for the contract");
        section
            .lines()
            .take_while(|line| !line.starts_with('['))
            .filter(|line| !line.is_empty())
            .collect()
    }

    /// Each interface the chain client declares by hand: its name, the contract it is a part
    /// of, and its declarations as the `sol!` macro read them.
    fn declared_by_hand() -> [(&'static str, &'static str, JsonAbi); 3] {
        [
            (
                "IRecurringCollector",
                "RecurringCollector",
                IRecurringCollector::abi::contract(),
            ),
            (
                "IRecurringAgreementManager",
                "RecurringAgreementManager",
                IRecurringAgreementManager::abi::contract(),
            ),
            (
                "IAccessControl",
                "RecurringAgreementManager",
                IAccessControl::abi::contract(),
            ),
        ]
    }

    /// A type the way the snapshot writes it: a struct as the tuple of its fields.
    fn kind(kind: &str, fields: &[Param]) -> String {
        let Some(array) = kind.strip_prefix("tuple") else {
            return kind.to_string();
        };
        let fields: Vec<_> = fields
            .iter()
            .map(|field| {
                format!(
                    "{} {}",
                    self::kind(&field.ty, &field.components),
                    field.name
                )
            })
            .collect();
        format!("({}){array}", fields.join(", "))
    }

    /// A parameter list the way the snapshot writes it: each parameter's type, then its name
    /// where it has one.
    fn parameters(parameters: &[Param]) -> String {
        let written: Vec<_> = parameters
            .iter()
            .map(|parameter| {
                let kind = kind(&parameter.ty, &parameter.components);
                format!("{kind} {}", parameter.name).trim().to_string()
            })
            .collect();
        written.join(", ")
    }

    /// Every function, event and error of an interface, the way the snapshot writes them.
    fn declarations(interface: &JsonAbi) -> Vec<String> {
        let functions = interface.functions().map(|function| {
            let mut written = format!(
                "function {}({})",
                function.name,
                parameters(&function.inputs)
            );
            written += match function.state_mutability {
                StateMutability::View => " view",
                StateMutability::Pure => " pure",
                _ => "",
            };
            if !function.outputs.is_empty() {
                written += &format!(" returns ({})", parameters(&function.outputs));
            }
            written
        });
        let events = interface.events().map(|event| {
            let inputs: Vec<_> = event
                .inputs
                .iter()
                .map(|input| {
                    let indexed = if input.indexed { " indexed" } else { "" };
                    let kind = kind(&input.ty, &input.components);
                    format!("{kind}{indexed} {}", input.name).trim().to_string()
                })
                .collect();
            format!("event {}({})", event.name, inputs.join(", "))
        });
        let errors = interface
            .errors()
            .map(|error| format!("error {}({})", error.name, parameters(&error.inputs)));
        functions.chain(events).chain(errors).collect()
    }

    /// Every function, event and error declared by hand exists in the contracts exactly as
    /// declared: its name, each parameter's type and name, which event parameters are indexed,
    /// whether it only reads, and what it returns.
    #[test]
    fn every_declaration_matches_the_contracts() {
        for (interface, contract, declared) in declared_by_hand() {
            let in_the_contracts = in_the_contracts(contract);
            let declarations = declarations(&declared);
            assert!(!declarations.is_empty(), "{interface} declares nothing");

            let drifted: Vec<_> = declarations
                .iter()
                .filter(|declared| !in_the_contracts.contains(&declared.as_str()))
                .collect();

            assert!(
                drifted.is_empty(),
                "{interface} declares what {contract} does not have: {drifted:#?}"
            );
        }
    }

    /// The check above only covers the interfaces it lists, so a new one has to be added to it.
    #[test]
    fn no_interface_declared_by_hand_is_left_unchecked() {
        let interfaces = [include_str!("abi.rs"), include_str!("manager_role.rs")]
            .iter()
            .flat_map(|source| source.lines())
            .filter(|line| line.trim_start().starts_with("interface "))
            .count();

        assert_eq!(
            interfaces,
            declared_by_hand().len(),
            "an interface in the chain client is missing from `declared_by_hand`"
        );
    }

    /// The agreement dipper signs and posts is declared by hand too, in `dipper-rpc`. Its fields
    /// are those the collector's `accept` takes, in the same order.
    #[test]
    fn the_agreement_matches_the_contracts() {
        let declared = RecurringCollectionAgreement::eip712_root_type();
        let fields = declared
            .strip_prefix("RecurringCollectionAgreement(")
            .and_then(|fields| fields.strip_suffix(')'))
            .expect("the type is written as its name, then its fields in brackets")
            .replace(',', ", ");
        let accept = format!(
            "function accept(({fields}) rca, bytes signature) returns (bytes16 agreementId)"
        );

        assert!(
            in_the_contracts("RecurringCollector").contains(&accept.as_str()),
            "the collector has no `{accept}`"
        );
    }

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
