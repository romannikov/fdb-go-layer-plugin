use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::RwLock;

pub use async_trait::async_trait;
pub use foundationdb;
pub use foundationdb::directory::{Directory, DirectoryError, DirectoryLayer, DirectoryOutput, DirectorySubspace};
pub use foundationdb::options::MutationType;
pub use foundationdb::{Database, FdbBindingError, FdbError, KeySelector, RangeOption, Transaction};
pub use foundationdb_tuple;
pub use foundationdb_tuple::{Bytes, Element, PackError, Subspace, TuplePack, TupleUnpack, Versionstamp};
pub use futures;
pub use num_bigint::BigInt;
pub use prost;

/// Partition namespaces under the typeID prefix (100% compatible with Go `fdblayer`).
pub const DATA_NAMESPACE: i64 = 0;
pub const INDEX_NAMESPACE: i64 = 1;
pub const FIELD_NAMESPACE: i64 = 2;

/// Errors returned by `fdb-layer` operations.
#[derive(Debug)]
pub enum FdbLayerError {
    AlreadyExists(&'static str),
    NotFound(&'static str),
    MetadataNotInitialized,
    TypeNotFound(String),
    Fdb(FdbError),
    Pack(PackError),
    Decode(prost::DecodeError),
    Encode(prost::EncodeError),
    Directory(DirectoryError),
    Custom(String),
}

impl FdbLayerError {
    pub fn is_already_exists(&self) -> bool {
        matches!(self, FdbLayerError::AlreadyExists(_))
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, FdbLayerError::NotFound(_))
    }
}

impl fmt::Display for FdbLayerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FdbLayerError::AlreadyExists(entity) => write!(f, "{} already exists", entity),
            FdbLayerError::NotFound(entity) => write!(f, "{} not found", entity),
            FdbLayerError::MetadataNotInitialized => {
                write!(f, "metadata not initialized, call sync_metadata first")
            }
            FdbLayerError::TypeNotFound(name) => write!(f, "type {} not found in metadata", name),
            FdbLayerError::Fdb(err) => write!(f, "fdb error: {}", err),
            FdbLayerError::Pack(err) => write!(f, "tuple pack/unpack error: {}", err),
            FdbLayerError::Decode(err) => write!(f, "protobuf decode error: {}", err),
            FdbLayerError::Encode(err) => write!(f, "protobuf encode error: {}", err),
            FdbLayerError::Directory(err) => write!(f, "directory error: {:?}", err),
            FdbLayerError::Custom(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for FdbLayerError {}

impl From<FdbError> for FdbLayerError {
    fn from(err: FdbError) -> Self {
        FdbLayerError::Fdb(err)
    }
}

impl From<PackError> for FdbLayerError {
    fn from(err: PackError) -> Self {
        FdbLayerError::Pack(err)
    }
}

impl From<prost::DecodeError> for FdbLayerError {
    fn from(err: prost::DecodeError) -> Self {
        FdbLayerError::Decode(err)
    }
}

impl From<prost::EncodeError> for FdbLayerError {
    fn from(err: prost::EncodeError) -> Self {
        FdbLayerError::Encode(err)
    }
}

impl From<DirectoryError> for FdbLayerError {
    fn from(err: DirectoryError) -> Self {
        FdbLayerError::Directory(err)
    }
}

impl From<FdbLayerError> for FdbBindingError {
    fn from(err: FdbLayerError) -> Self {
        match err {
            FdbLayerError::Fdb(fdb_err) => FdbBindingError::from(fdb_err),
            other => FdbBindingError::new_custom_error(Box::new(other)),
        }
    }
}

/// Generic repository trait for CRUD operations on entity `T` with primary key `PK`.
#[async_trait]
pub trait GenericRepository<T, PK>: Send + Sync {
    async fn create(
        &self,
        tr: &Transaction,
        dir: &Subspace,
        entity: &T,
    ) -> Result<(), FdbLayerError>;
    async fn get(&self, tr: &Transaction, dir: &Subspace, pk: PK) -> Result<T, FdbLayerError>;
    async fn set(&self, tr: &Transaction, dir: &Subspace, entity: &T) -> Result<(), FdbLayerError>;
    async fn delete(&self, tr: &Transaction, dir: &Subspace, pk: PK) -> Result<(), FdbLayerError>;
}

/// Options for paginated `list_*` queries.
#[derive(Debug, Clone, Default)]
pub struct PaginationOptions {
    pub begin: Vec<Element<'static>>,
    pub limit: usize,
}

/// Paginated result set returned by `list_*` queries.
#[derive(Debug, Clone)]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub next_key: Vec<Element<'static>>,
    pub has_more: bool,
}

impl<T> Default for PaginatedResult<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            next_key: Vec::new(),
            has_more: false,
        }
    }
}

/// Computes the exclusive upper bound key for a prefix range (matching Go `fdb.PrefixRange`).
pub fn strinc(prefix: &[u8]) -> Vec<u8> {
    let mut key = prefix.to_vec();
    for i in (0..key.len()).rev() {
        if key[i] != 0xff {
            key[i] += 1;
            return key;
        }
        key.pop();
    }
    vec![0xff]
}

/// Returns `(begin, end)` key range matching Go `fdb.PrefixRange(prefix)`.
pub fn prefix_range(prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (prefix.to_vec(), strinc(prefix))
}

/// Builds a `RangeOption` over all keys starting with `prefix`.
pub fn prefix_range_option(prefix: &[u8], limit: Option<usize>) -> RangeOption<'static> {
    let (begin, end) = prefix_range(prefix);
    RangeOption {
        begin: KeySelector::first_greater_or_equal(begin),
        end: KeySelector::first_greater_or_equal(end),
        limit,
        ..RangeOption::default()
    }
}

/// Converts a `u64` into an `Element<'static>` using the exact same signed/unsigned
/// tuple representation as Go (`int64` when `<= i64::MAX`, `BigInt` when `> i64::MAX`).
pub fn element_from_u64(v: u64) -> Element<'static> {
    if v <= i64::MAX as u64 {
        Element::Int(v as i64)
    } else {
        Element::BigInt(BigInt::from(v))
    }
}

/// Formats a tuple slice in the same format as Go `tuple.Tuple.String()`, e.g. `("u1")` or `("a", 42)`.
pub fn format_tuple(elements: &[Element<'_>]) -> String {
    let mut out = String::from("(");
    for (i, el) in elements.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        match el {
            Element::Nil => out.push_str("<nil>"),
            Element::String(s) => out.push_str(&format!("{:?}", s.as_ref())),
            Element::Int(v) => out.push_str(&v.to_string()),
            Element::BigInt(v) => out.push_str(&v.to_string()),
            Element::Bool(v) => out.push_str(&v.to_string()),
            Element::Float(v) => out.push_str(&v.to_string()),
            Element::Double(v) => out.push_str(&v.to_string()),
            Element::Bytes(b) => out.push_str(&format!("{}", b)),
            Element::Versionstamp(vs) => out.push_str(&format!("{:?}", vs)),
            Element::Tuple(inner) => out.push_str(&format_tuple(inner)),
            _ => out.push_str(&format!("{:?}", el)),
        }
    }
    out.push(')');
    out
}

/// `RecordStore` holds metadata mapping between message names and their integer type IDs.
pub struct RecordStore {
    metadata: RwLock<Option<HashMap<String, i64>>>,
    user_version_seq: AtomicU32,
}

impl Default for RecordStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordStore {
    /// Creates a new `RecordStore` instance.
    pub fn new() -> Self {
        Self {
            metadata: RwLock::new(None),
            user_version_seq: AtomicU32::new(0),
        }
    }

    /// Returns a monotonically increasing 16-bit user version so that multiple
    /// versionstamped keys written within the same transaction receive distinct
    /// user versions instead of colliding at 0.
    pub fn next_user_version(&self) -> u16 {
        self.user_version_seq.fetch_add(1, Ordering::Relaxed) as u16
    }

    /// Retrieves the type ID for a given message name.
    pub fn get_type_id(&self, name: &str) -> Result<i64, FdbLayerError> {
        let guard = self.metadata.read().unwrap();
        let map = guard.as_ref().ok_or(FdbLayerError::MetadataNotInitialized)?;
        map.get(name)
            .copied()
            .ok_or_else(|| FdbLayerError::TypeNotFound(name.to_string()))
    }

    /// Returns a cloned copy of the metadata mapping.
    pub fn metadata(&self) -> HashMap<String, i64> {
        let guard = self.metadata.read().unwrap();
        guard.clone().unwrap_or_default()
    }

    /// Reads the existing metadata from FDB and assigns new IDs to any unmapped messages.
    /// Binary-compatible with Go `RecordStore.SyncMetadata`.
    pub async fn sync_metadata(
        &self,
        tr: &Transaction,
        meta_dir: &Subspace,
        messages: &[&str],
    ) -> Result<(), FdbLayerError> {
        use futures::TryStreamExt;

        let mut map = HashMap::with_capacity(messages.len());
        let range_opt = prefix_range_option(meta_dir.bytes(), None);
        let kvs: Vec<_> = tr.get_ranges_keyvalues(range_opt, false).try_collect().await?;

        let mut max_id: i64 = 0;
        for kv in &kvs {
            let tpl = match meta_dir.unpack::<Vec<Element<'_>>>(kv.key()) {
                Ok(t) if t.len() == 1 => t,
                _ => continue,
            };
            let msg_name = match tpl[0].as_str() {
                Some(s) => s.to_string(),
                None => continue,
            };
            let val_tpl = match foundationdb_tuple::unpack::<Vec<Element<'_>>>(kv.value()) {
                Ok(t) if t.len() == 1 => t,
                _ => continue,
            };
            let id = match val_tpl[0].as_i64() {
                Some(v) => v,
                None => continue,
            };
            map.insert(msg_name, id);
            if id > max_id {
                max_id = id;
            }
        }

        for &msg in messages {
            if map.contains_key(msg) {
                continue;
            }
            let key = meta_dir.pack(&(msg,));
            if let Some(raw) = tr.get(&key, false).await? {
                if let Ok(val_tpl) = foundationdb_tuple::unpack::<Vec<Element<'_>>>(&raw) {
                    if val_tpl.len() == 1 {
                        if let Some(id) = val_tpl[0].as_i64() {
                            map.insert(msg.to_string(), id);
                            if id > max_id {
                                max_id = id;
                            }
                            continue;
                        }
                    }
                }
            }
            max_id += 1;
            map.insert(msg.to_string(), max_id);
            let val = foundationdb_tuple::pack(&(max_id,));
            tr.set(&key, &val);
        }

        let mut guard = self.metadata.write().unwrap();
        *guard = Some(map);
        Ok(())
    }
}

/// Converts a 12-byte slice into a complete `Versionstamp`.
pub fn bytes_to_versionstamp(b: &[u8]) -> Versionstamp {
    let mut tr_version = [0u8; 10];
    let copy_len = b.len().min(10);
    tr_version[..copy_len].copy_from_slice(&b[..copy_len]);
    let user_version = if b.len() >= 12 {
        u16::from_be_bytes([b[10], b[11]])
    } else {
        0
    };
    Versionstamp::complete(tr_version, user_version)
}
