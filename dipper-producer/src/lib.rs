pub mod events;
pub mod kafka;
pub mod proto;

// Re-exported so consumers of the generated types decode without pinning
// their own copy of prost.
pub use prost;
