use anyhow::anyhow;
use std::sync::Arc;
use std::sync::RwLock;

struct SqliteLogAppender {
    buf: Arc<RwLock<Vec<LogRecord>>>,
    buf_size: usize,
    file_name: String,
}

struct LogRecord {
    id: String,
    level: String,
    ts: String,
    message: String,
}

impl SqliteLogAppender {
    pub fn new(buf_size: usize, file_name: &str) -> anyhow::Result<SqliteLogAppender> {
        Ok(SqliteLogAppender {
            buf: Arc::new(RwLock::new(Vec::new())),
            buf_size,
            file_name: file_name.to_string(),
        })
    }
    fn create_entry_table_if_not_exists(conn: &rusqlite::Connection) -> anyhow::Result<()> {
        let table_sql = "create table if not exists entry (
            id varchar(128) not null primary key,
            ts varchar(128) not null,
            level varchar(128) not null,
            message varchar(8192) not null
        )";
        let index_ts_sql = "create index if not exists entry_ts_i on entry (ts)";
        conn.execute(table_sql, [])?;
        conn.execute(index_ts_sql, [])?;
        Ok(())
    }
    fn connect(&self) -> anyhow::Result<rusqlite::Connection> {
        let conn = rusqlite::Connection::open(&self.file_name)?;
        SqliteLogAppender::create_entry_table_if_not_exists(&conn)?;
        Ok(conn)
    }
    fn maybe_flush_buf(&self, buf_lock: &mut Vec<LogRecord>) -> anyhow::Result<()> {
        if buf_lock.len() < self.buf_size {
            return Ok(());
        }
        self.flush_buf(buf_lock)?;
        Ok(())
    }
    fn flush_buf(&self, buf_lock: &mut Vec<LogRecord>) -> anyhow::Result<()> {
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;
        {
            let mut stmt =
                tx.prepare("insert into entry (id, ts, level, message) values (?1, ?2, ?3, ?4)")?;
            for lr in buf_lock.iter() {
                stmt.execute([&lr.id, &lr.ts, &lr.level, &lr.message])?;
            }
        }
        tx.commit()?;
        buf_lock.clear();
        Ok(())
    }
}

impl std::fmt::Debug for SqliteLogAppender {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SqliteLogAppender")
            .field("buf_size", &self.buf_size)
            .field("file_name", &self.file_name)
            .finish()
    }
}

impl log4rs::append::Append for SqliteLogAppender {
    fn append(&self, record: &log::Record) -> anyhow::Result<()> {
        let lr = LogRecord {
            id: uuid::Uuid::new_v4().to_string(),
            level: record.level().as_str().to_string(),
            ts: chrono::Utc::now()
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string(),
            message: record.args().to_string(),
        };
        let mut buf_lock = self
            .buf
            .write()
            .map_err(|e| anyhow!("Error locking buf: {}", e))?;
        buf_lock.push(lr);
        self.maybe_flush_buf(&mut buf_lock)?;
        Ok(())
    }
    fn flush(&self) {
        let mut buf_lock = self.buf.write().expect("Error locking buf");
        self.flush_buf(&mut buf_lock).expect("Error flushing buf");
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteLogAppenderConfig {
    path: String,
}

pub struct SqliteLogAppenderDeserializer {}

impl log4rs::config::Deserialize for SqliteLogAppenderDeserializer {
    type Trait = dyn log4rs::append::Append;
    type Config = SqliteLogAppenderConfig;

    fn deserialize(
        &self,
        config: SqliteLogAppenderConfig,
        _: &log4rs::config::Deserializers,
    ) -> anyhow::Result<Box<dyn log4rs::append::Append>> {
        Ok(Box::new(SqliteLogAppender::new(1024, &config.path)?))
    }
}
