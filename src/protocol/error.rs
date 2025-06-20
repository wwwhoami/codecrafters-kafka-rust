#[derive(Debug, Clone)]
pub(crate) struct UnsupportedApiKeyError {
    key: i16,
}

impl UnsupportedApiKeyError {
    pub(crate) fn new(key: i16) -> Self {
        Self { key }
    }
}

impl std::fmt::Display for UnsupportedApiKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unsupported API key {}", self.key)
    }
}

impl std::error::Error for UnsupportedApiKeyError {}

#[derive(Debug, Clone)]
pub(crate) struct IoError {
    message: String,
}

impl IoError {
    pub(crate) fn new(message: String) -> Self {
        Self { message }
    }
}

impl std::fmt::Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "I/O error: {}", self.message)
    }
}

impl std::error::Error for IoError {}
