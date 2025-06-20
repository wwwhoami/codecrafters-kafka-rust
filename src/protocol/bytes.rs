pub trait ToBytes {
    fn to_be_bytes(&self) -> Vec<u8>;
}

pub trait FromBytes: Sized {
    fn from_be_bytes<B: bytes::Buf>(buf: &mut B) -> crate::Result<Self>;
}
