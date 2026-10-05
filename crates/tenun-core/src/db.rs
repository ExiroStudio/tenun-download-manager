//! SQLite persistence layer for download records, queue ordering, and crash recovery.

use crate::category::FileCategory;
use crate::error::{Result, TenunError};
use crate::probe::RequestOptions;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Lifecycle state of a download in the queue and manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadStatus {
    /// Waiting in queue to be started when a worker slot is available.
    Queued,
    /// Probing remote resource (HEAD/GET range probe).
    Probing,
    /// Actively transferring data chunks.
    Downloading,
    /// Paused by user or checkpointed before shutdown.
    Paused,
    /// Verifying file checksum integrity.
    Verifying,
    /// Successfully finished and verified.
    Completed,
    /// Failed due to an unrecoverable error.
    Failed,
    /// Cancelled and aborted by user.
    Cancelled,
    /// Remote resource expired or requires refresh (401/403).
    NeedsRefresh,
}

impl DownloadStatus {
    /// String representation stored in SQLite.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "Queued",
            Self::Probing => "Probing",
            Self::Downloading => "Downloading",
            Self::Paused => "Paused",
            Self::Verifying => "Verifying",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::NeedsRefresh => "NeedsRefresh",
        }
    }

    /// Parses status string from database.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Queued" => Some(Self::Queued),
            "Probing" => Some(Self::Probing),
            "Downloading" => Some(Self::Downloading),
            "Paused" => Some(Self::Paused),
            "Verifying" => Some(Self::Verifying),
            "Completed" => Some(Self::Completed),
            "Failed" => Some(Self::Failed),
            "Cancelled" => Some(Self::Cancelled),
            "NeedsRefresh" => Some(Self::NeedsRefresh),
            _ => None,
        }
    }

    /// Whether this status counts as an active worker connection.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Probing | Self::Downloading | Self::Verifying)
    }

    /// Whether this status can be transitioned to active execution.
    pub fn is_resumable(&self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Paused | Self::Failed | Self::NeedsRefresh
        )
    }
}

impl std::fmt::Display for DownloadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Persistent download record loaded from SQLite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRecord {
    /// Numerical unique identifier (autoincrement).
    pub id: u64,
    /// UUID v4 string identifier.
    pub uuid: String,
    /// Original URL requested.
    pub url: String,
    /// Final URL after redirects.
    pub final_url: Option<String>,
    /// Filename of target output.
    pub filename: String,
    /// Target destination directory.
    pub dest_dir: String,
    /// Total expected file size in bytes, if known.
    pub total_bytes: Option<u64>,
    /// Bytes downloaded so far.
    pub downloaded_bytes: u64,
    /// Current lifecycle status.
    pub status: DownloadStatus,
    /// File category (Documents, Video, Compressed, etc.).
    pub category: FileCategory,
    /// Queue priority (higher number runs earlier).
    pub priority: i32,
    /// Remote ETag validator.
    pub etag: Option<String>,
    /// Remote Last-Modified validator.
    pub last_modified: Option<String>,
    /// Created timestamp (Unix seconds).
    pub created_at: u64,
    /// Last updated timestamp (Unix seconds).
    pub updated_at: u64,
    /// Completed timestamp (Unix seconds), if finished.
    pub completed_at: Option<u64>,
    /// Failure error message, if any.
    pub error_message: Option<String>,
    /// Serialized JSON string of request options.
    pub options_json: String,
}

impl DownloadRecord {
    /// Deserializes `RequestOptions` stored in this record.
    pub fn options(&self) -> RequestOptions {
        serde_json::from_str(&self.options_json).unwrap_or_default()
    }
}

/// Specification for inserting a new download item.
#[derive(Debug, Clone)]
pub struct NewDownloadRecord {
    /// Unique UUID string.
    pub uuid: String,
    /// Target URL to download.
    pub url: String,
    /// Initial suggested filename.
    pub filename: String,
    /// Target directory.
    pub dest_dir: String,
    /// Expected file size, if known ahead of time.
    pub total_bytes: Option<u64>,
    /// Initial queue status (typically Queued or Paused).
    pub status: DownloadStatus,
    /// Category classification.
    pub category: FileCategory,
    /// Queue priority.
    pub priority: i32,
    /// Request options.
    pub options: RequestOptions,
}

impl NewDownloadRecord {
    /// Creates a new download record specification with generated UUID.
    pub fn new(
        url: impl Into<String>,
        filename: impl Into<String>,
        dest_dir: impl Into<String>,
        category: FileCategory,
        status: DownloadStatus,
        priority: i32,
        options: RequestOptions,
    ) -> Self {
        Self {
            uuid: uuid::Uuid::new_v4().to_string(),
            url: url.into(),
            filename: filename.into(),
            dest_dir: dest_dir.into(),
            total_bytes: None,
            status,
            category,
            priority,
            options,
        }
    }
}

/// Thread-safe SQLite database manager for Tenun downloads.
#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    /// Opens or creates an SQLite database at `path`, enabling WAL mode.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(TenunError::Io)?;
        }

        let conn = Connection::open(path).map_err(|e| TenunError::Database(e.to_string()))?;

        // Configure SQLite performance and reliability pragmas
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;",
        )
        .map_err(|e| TenunError::Database(e.to_string()))?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.init_tables()?;
        Ok(db)
    }

    /// Creates an in-memory database for testing.
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| TenunError::Database(e.to_string()))?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.init_tables()?;
        Ok(db)
    }

    /// Initializes schema tables and indexes.
    pub fn init_tables(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL UNIQUE,
                url TEXT NOT NULL,
                final_url TEXT,
                filename TEXT NOT NULL,
                dest_dir TEXT NOT NULL,
                total_bytes INTEGER,
                downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                category TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                etag TEXT,
                last_modified TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                error_message TEXT,
                options_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
            CREATE INDEX IF NOT EXISTS idx_downloads_priority ON downloads(priority DESC, created_at ASC);
            CREATE INDEX IF NOT EXISTS idx_downloads_category ON downloads(category);",
        )
        .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(())
    }

    /// Recovers interrupted downloads on application startup:
    /// any downloads in active states (`Downloading`, `Probing`, `Verifying`)
    /// are transitioned cleanly to `Paused`.
    pub fn recover_interrupted_downloads(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let count = conn
            .execute(
                "UPDATE downloads 
                 SET status = 'Paused', updated_at = ?1
                 WHERE status IN ('Downloading', 'Probing', 'Verifying')",
                params![now],
            )
            .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(count)
    }

    /// Inserts a new download record, returning its numeric ID.
    pub fn insert(&self, record: &NewDownloadRecord) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let options_json =
            serde_json::to_string(&record.options).unwrap_or_else(|_| "{}".to_string());

        conn.execute(
            "INSERT INTO downloads (
                uuid, url, final_url, filename, dest_dir, total_bytes, downloaded_bytes,
                status, category, priority, etag, last_modified, created_at, updated_at,
                completed_at, error_message, options_json
            ) VALUES (
                ?1, ?2, NULL, ?3, ?4, ?5, 0,
                ?6, ?7, ?8, NULL, NULL, ?9, ?9,
                NULL, NULL, ?10
            )",
            params![
                record.uuid,
                record.url,
                record.filename,
                record.dest_dir,
                record.total_bytes.map(|b| b as i64),
                record.status.as_str(),
                record.category.subfolder_name(),
                record.priority,
                now,
                options_json,
            ],
        )
        .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(conn.last_insert_rowid() as u64)
    }

    /// Updates status and optional error message.
    pub fn update_status(
        &self,
        id: u64,
        status: DownloadStatus,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        conn.execute(
            "UPDATE downloads SET status = ?1, error_message = ?2, updated_at = ?3 WHERE id = ?4",
            params![status.as_str(), error, now, id as i64],
        )
        .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(())
    }

    /// Updates progress counters.
    pub fn update_progress(&self, id: u64, downloaded: u64, total: Option<u64>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        conn.execute(
            "UPDATE downloads 
             SET downloaded_bytes = ?1, total_bytes = coalesce(?2, total_bytes), updated_at = ?3 
             WHERE id = ?4",
            params![downloaded as i64, total.map(|t| t as i64), now, id as i64],
        )
        .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(())
    }

    /// Updates probed metadata (filename, final URL, total size, ETag, Last-Modified, category).
    pub fn update_metadata(
        &self,
        id: u64,
        filename: &str,
        final_url: Option<&str>,
        total_bytes: Option<u64>,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let category = FileCategory::from_filename_or_mime(filename, None);

        conn.execute(
            "UPDATE downloads 
             SET filename = ?1, final_url = ?2, total_bytes = coalesce(?3, total_bytes),
                 etag = coalesce(?4, etag), last_modified = coalesce(?5, last_modified),
                 category = ?6, updated_at = ?7
             WHERE id = ?8",
            params![
                filename,
                final_url,
                total_bytes.map(|t| t as i64),
                etag,
                last_modified,
                category.subfolder_name(),
                now,
                id as i64,
            ],
        )
        .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(())
    }

    /// Marks a download as completed successfully.
    pub fn mark_completed(&self, id: u64, final_bytes: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        conn.execute(
            "UPDATE downloads 
             SET status = 'Completed', downloaded_bytes = ?1, total_bytes = ?1, 
                 completed_at = ?2, updated_at = ?2, error_message = NULL
             WHERE id = ?3",
            params![final_bytes as i64, now, id as i64],
        )
        .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(())
    }

    /// Fetches a single download record by numeric ID.
    pub fn get(&self, id: u64) -> Result<Option<DownloadRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT * FROM downloads WHERE id = ?1")
            .map_err(|e| TenunError::Database(e.to_string()))?;

        stmt.query_row(params![id as i64], Self::map_row)
            .optional()
            .map_err(|e| TenunError::Database(e.to_string()))
    }

    /// Fetches a single download record by UUID string.
    pub fn get_by_uuid(&self, uuid: &str) -> Result<Option<DownloadRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT * FROM downloads WHERE uuid = ?1")
            .map_err(|e| TenunError::Database(e.to_string()))?;

        stmt.query_row(params![uuid], Self::map_row)
            .optional()
            .map_err(|e| TenunError::Database(e.to_string()))
    }

    /// Fetches all downloads matching optional status and category filters.
    pub fn list(
        &self,
        status_filter: Option<DownloadStatus>,
        category_filter: Option<FileCategory>,
    ) -> Result<Vec<DownloadRecord>> {
        let conn = self.conn.lock().unwrap();

        let mut query = "SELECT * FROM downloads WHERE 1=1".to_string();
        let mut param_values: Vec<rusqlite::types::Value> = Vec::new();

        if let Some(status) = status_filter {
            query.push_str(" AND status = ?");
            param_values.push(rusqlite::types::Value::Text(status.as_str().to_string()));
        }

        if let Some(cat) = category_filter {
            query.push_str(" AND category = ?");
            param_values.push(rusqlite::types::Value::Text(
                cat.subfolder_name().to_string(),
            ));
        }

        query.push_str(" ORDER BY created_at DESC");

        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| TenunError::Database(e.to_string()))?;

        let params_refs: Vec<&dyn rusqlite::ToSql> = param_values
            .iter()
            .map(|v| v as &dyn rusqlite::ToSql)
            .collect();

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::map_row)
            .map_err(|e| TenunError::Database(e.to_string()))?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(|e| TenunError::Database(e.to_string()))?);
        }

        Ok(results)
    }

    /// Retrieves the next queued download item ordered by priority DESC, created_at ASC.
    pub fn get_next_queued(&self) -> Result<Option<DownloadRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT * FROM downloads 
                 WHERE status = 'Queued' 
                 ORDER BY priority DESC, created_at ASC 
                 LIMIT 1",
            )
            .map_err(|e| TenunError::Database(e.to_string()))?;

        stmt.query_row([], Self::map_row)
            .optional()
            .map_err(|e| TenunError::Database(e.to_string()))
    }

    /// Counts how many downloads are currently in active transfer states.
    pub fn count_active(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM downloads WHERE status IN ('Probing', 'Downloading', 'Verifying')",
                [],
                |r| r.get(0),
            )
            .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(count as usize)
    }

    /// Deletes a download record from the database.
    pub fn delete(&self, id: u64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn
            .execute("DELETE FROM downloads WHERE id = ?1", params![id as i64])
            .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(rows > 0)
    }

    /// Updates priority of a download item.
    pub fn update_priority(&self, id: u64, priority: i32) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        conn.execute(
            "UPDATE downloads SET priority = ?1, updated_at = ?2 WHERE id = ?3",
            params![priority, now, id as i64],
        )
        .map_err(|e| TenunError::Database(e.to_string()))?;

        Ok(())
    }

    fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DownloadRecord> {
        let id: i64 = row.get("id")?;
        let uuid: String = row.get("uuid")?;
        let url: String = row.get("url")?;
        let final_url: Option<String> = row.get("final_url")?;
        let filename: String = row.get("filename")?;
        let dest_dir: String = row.get("dest_dir")?;
        let total_bytes: Option<i64> = row.get("total_bytes")?;
        let downloaded_bytes: i64 = row.get("downloaded_bytes")?;
        let status_str: String = row.get("status")?;
        let category_str: String = row.get("category")?;
        let priority: i32 = row.get("priority")?;
        let etag: Option<String> = row.get("etag")?;
        let last_modified: Option<String> = row.get("last_modified")?;
        let created_at: i64 = row.get("created_at")?;
        let updated_at: i64 = row.get("updated_at")?;
        let completed_at: Option<i64> = row.get("completed_at")?;
        let error_message: Option<String> = row.get("error_message")?;
        let options_json: String = row.get("options_json")?;

        let status = DownloadStatus::parse(&status_str).unwrap_or(DownloadStatus::Queued);
        let category = category_str.parse::<FileCategory>().unwrap_or_default();

        Ok(DownloadRecord {
            id: id as u64,
            uuid,
            url,
            final_url,
            filename,
            dest_dir,
            total_bytes: total_bytes.map(|b| b as u64),
            downloaded_bytes: downloaded_bytes as u64,
            status,
            category,
            priority,
            etag,
            last_modified,
            created_at: created_at as u64,
            updated_at: updated_at as u64,
            completed_at: completed_at.map(|c| c as u64),
            error_message,
            options_json,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_and_crud() {
        let db = Database::in_memory().expect("in memory db");

        let new_rec = NewDownloadRecord {
            uuid: "test-uuid-1".to_string(),
            url: "https://example.com/archive.zip".to_string(),
            filename: "archive.zip".to_string(),
            dest_dir: "/tmp".to_string(),
            total_bytes: Some(1024),
            status: DownloadStatus::Queued,
            category: FileCategory::Compressed,
            priority: 5,
            options: RequestOptions::default(),
        };

        let id = db.insert(&new_rec).expect("insert");
        assert!(id > 0);

        let rec = db.get(id).expect("get").expect("found");
        assert_eq!(rec.uuid, "test-uuid-1");
        assert_eq!(rec.status, DownloadStatus::Queued);
        assert_eq!(rec.priority, 5);

        // Update progress
        db.update_progress(id, 512, Some(1024))
            .expect("update progress");
        let rec2 = db.get(id).expect("get").expect("found");
        assert_eq!(rec2.downloaded_bytes, 512);

        // Update status
        db.update_status(id, DownloadStatus::Downloading, None)
            .expect("status update");
        assert_eq!(db.count_active().expect("count active"), 1);

        // Mark completed
        db.mark_completed(id, 1024).expect("mark completed");
        let rec3 = db.get(id).expect("get").expect("found");
        assert_eq!(rec3.status, DownloadStatus::Completed);
        assert_eq!(rec3.downloaded_bytes, 1024);
        assert!(rec3.completed_at.is_some());

        // Delete
        assert!(db.delete(id).expect("delete"));
        assert!(db.get(id).expect("get").is_none());
    }

    #[test]
    fn test_queue_priority_ordering() {
        let db = Database::in_memory().expect("in memory db");

        let rec1 = NewDownloadRecord {
            uuid: "low-priority".to_string(),
            url: "https://example.com/low.zip".to_string(),
            filename: "low.zip".to_string(),
            dest_dir: "/tmp".to_string(),
            total_bytes: None,
            status: DownloadStatus::Queued,
            category: FileCategory::General,
            priority: 0,
            options: RequestOptions::default(),
        };

        let rec2 = NewDownloadRecord {
            uuid: "high-priority".to_string(),
            url: "https://example.com/high.zip".to_string(),
            filename: "high.zip".to_string(),
            dest_dir: "/tmp".to_string(),
            total_bytes: None,
            status: DownloadStatus::Queued,
            category: FileCategory::General,
            priority: 10,
            options: RequestOptions::default(),
        };

        db.insert(&rec1).expect("insert 1");
        db.insert(&rec2).expect("insert 2");

        let next = db.get_next_queued().expect("next").expect("found");
        assert_eq!(next.uuid, "high-priority");
    }

    #[test]
    fn test_crash_recovery() {
        let db = Database::in_memory().expect("in memory db");

        let rec1 = NewDownloadRecord {
            uuid: "active-downloading".to_string(),
            url: "https://example.com/a.zip".to_string(),
            filename: "a.zip".to_string(),
            dest_dir: "/tmp".to_string(),
            total_bytes: None,
            status: DownloadStatus::Downloading,
            category: FileCategory::General,
            priority: 0,
            options: RequestOptions::default(),
        };

        let rec2 = NewDownloadRecord {
            uuid: "already-paused".to_string(),
            url: "https://example.com/b.zip".to_string(),
            filename: "b.zip".to_string(),
            dest_dir: "/tmp".to_string(),
            total_bytes: None,
            status: DownloadStatus::Paused,
            category: FileCategory::General,
            priority: 0,
            options: RequestOptions::default(),
        };

        let id1 = db.insert(&rec1).expect("insert 1");
        let id2 = db.insert(&rec2).expect("insert 2");

        let recovered = db.recover_interrupted_downloads().expect("recover");
        assert_eq!(recovered, 1);

        assert_eq!(db.get(id1).unwrap().unwrap().status, DownloadStatus::Paused);
        assert_eq!(db.get(id2).unwrap().unwrap().status, DownloadStatus::Paused);
    }
}
