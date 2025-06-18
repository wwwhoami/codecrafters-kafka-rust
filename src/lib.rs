mod protocol;
mod server_async;
mod server_sync;

pub(crate) type Error = Box<dyn std::error::Error>;
pub(crate) type Result<T> = std::result::Result<T, Error>;

pub use server_async::ServerAsync;
pub use server_sync::ServerSync;
