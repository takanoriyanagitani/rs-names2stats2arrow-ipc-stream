use std::fs::Metadata;
use std::io;
use std::sync::Arc;
use std::time::SystemTime;

use io::BufRead;

use std::path::Path;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;

use arrow::array::Array;
use arrow::array::BooleanBuilder;
use arrow::array::StringBuilder;
use arrow::array::TimestampSecondBuilder;
use arrow::array::UInt32Builder;
use arrow::array::UInt64Builder;

use arrow::record_batch::RecordBatch;

pub fn path2meta<P>(p: P) -> Result<Metadata, io::Error>
where
    P: AsRef<Path>,
{
    std::fs::metadata(p)
}

pub enum FileType {
    Dir,
    File,
    Symlink,
    Unspecified,
}

impl FileType {
    pub fn name(&self) -> &str {
        match self {
            FileType::Dir => "dir",
            FileType::File => "file",
            FileType::Symlink => "symlink",
            FileType::Unspecified => "unspecified",
        }
    }
}

pub struct FileMeta<'a>(pub &'a Metadata);

impl<'a> FileMeta<'a> {
    pub fn file_type(&self) -> FileType {
        let ft = self.0.file_type();
        if ft.is_dir() {
            FileType::Dir
        } else if ft.is_symlink() {
            FileType::Symlink
        } else if ft.is_file() {
            FileType::File
        } else {
            FileType::Unspecified
        }
    }

    pub fn accessed(&self) -> Result<SystemTime, io::Error> {
        self.0.accessed()
    }

    pub fn read_only(&self) -> bool {
        self.0.permissions().readonly()
    }
}

#[cfg(unix)]
impl<'a> FileMeta<'a> {
    pub fn mode(&self) -> u32 {
        std::os::unix::fs::MetadataExt::mode(self.0)
    }
    pub fn nlink(&self) -> u64 {
        std::os::unix::fs::MetadataExt::nlink(self.0)
    }
    pub fn uid(&self) -> u32 {
        std::os::unix::fs::MetadataExt::uid(self.0)
    }
    pub fn gid(&self) -> u32 {
        std::os::unix::fs::MetadataExt::gid(self.0)
    }
}

pub fn stdin2lines() -> impl Iterator<Item = Result<String, io::Error>> {
    io::stdin().lock().lines()
}

pub fn schema() -> SchemaRef {
    Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("read_only", DataType::Boolean, false),
        Field::new("mode", DataType::UInt32, true),
        Field::new("nlink", DataType::UInt64, true),
        Field::new("len", DataType::UInt64, false),
        Field::new("uid", DataType::UInt32, true),
        Field::new("gid", DataType::UInt32, true),
        Field::new("mtime", DataType::Timestamp(TimeUnit::Second, None), true),
    ])
    .into()
}

#[cfg(unix)]
pub fn lines2batch<I>(
    lines: &mut I,
    schema: SchemaRef,
    bldr: &mut Builder,
) -> Result<Option<RecordBatch>, io::Error>
where
    I: Iterator<Item = Result<String, io::Error>>,
{
    for rline in lines {
        let line: String = rline?;
        let meta: Metadata = path2meta(&line)?;
        let fmet = FileMeta(&meta);
        bldr.append_path(line);
        bldr.append_type(fmet.file_type().name());
        bldr.append_read_only(fmet.read_only());
        bldr.append_mode(Some(fmet.mode()));
        bldr.append_nlink(Some(fmet.nlink()));
        bldr.append_len(meta.len());
        bldr.append_uid(Some(fmet.uid()));
        bldr.append_gid(Some(fmet.gid()));
        let mtime_secs = meta
            .modified()?
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs() as i64);
        bldr.append_mtime(mtime_secs);
    }

    if bldr.is_empty() {
        return Ok(None);
    }

    let apath: Arc<dyn Array> = bldr.finish_path();
    let atype: Arc<dyn Array> = bldr.finish_type();
    let aread_only: Arc<dyn Array> = bldr.finish_read_only();
    let amode: Arc<dyn Array> = bldr.finish_mode();
    let anlink: Arc<dyn Array> = bldr.finish_nlink();
    let alen: Arc<dyn Array> = bldr.finish_len();
    let auid: Arc<dyn Array> = bldr.finish_uid();
    let agid: Arc<dyn Array> = bldr.finish_gid();
    let amtime: Arc<dyn Array> = bldr.finish_mtime();

    RecordBatch::try_new(
        schema,
        vec![
            apath, atype, aread_only, amode, anlink, alen, auid, agid, amtime,
        ],
    )
    .map_err(io::Error::other)
    .map(Some)
}

pub struct Builder {
    pub path: StringBuilder,
    pub file_type: StringBuilder,
    pub read_only: BooleanBuilder,
    pub mode: UInt32Builder,
    pub nlink: UInt64Builder,
    pub len: UInt64Builder,
    pub uid: UInt32Builder,
    pub gid: UInt32Builder,
    pub mtime: TimestampSecondBuilder,
}

impl Builder {
    pub fn append_path(&mut self, p: String) {
        self.path.append_value(p)
    }

    pub fn append_type(&mut self, t: &str) {
        self.file_type.append_value(t)
    }
    pub fn append_read_only(&mut self, r: bool) {
        self.read_only.append_value(r)
    }
    pub fn append_mode(&mut self, m: Option<u32>) {
        self.mode.append_option(m)
    }
    pub fn append_nlink(&mut self, l: Option<u64>) {
        self.nlink.append_option(l)
    }
    pub fn append_len(&mut self, l: u64) {
        self.len.append_value(l)
    }
    pub fn append_uid(&mut self, l: Option<u32>) {
        self.uid.append_option(l)
    }
    pub fn append_gid(&mut self, l: Option<u32>) {
        self.gid.append_option(l)
    }
    pub fn append_mtime(&mut self, t: Option<i64>) {
        self.mtime.append_option(t)
    }
}

impl Builder {
    pub fn is_empty(&self) -> bool {
        self.path.values_slice().is_empty()
    }
}

impl Builder {
    pub fn finish_path(&mut self) -> Arc<dyn Array> {
        Arc::new(self.path.finish())
    }

    pub fn finish_type(&mut self) -> Arc<dyn Array> {
        Arc::new(self.file_type.finish())
    }
    pub fn finish_read_only(&mut self) -> Arc<dyn Array> {
        Arc::new(self.read_only.finish())
    }
    pub fn finish_mode(&mut self) -> Arc<dyn Array> {
        Arc::new(self.mode.finish())
    }
    pub fn finish_nlink(&mut self) -> Arc<dyn Array> {
        Arc::new(self.nlink.finish())
    }
    pub fn finish_len(&mut self) -> Arc<dyn Array> {
        Arc::new(self.len.finish())
    }
    pub fn finish_uid(&mut self) -> Arc<dyn Array> {
        Arc::new(self.uid.finish())
    }
    pub fn finish_gid(&mut self) -> Arc<dyn Array> {
        Arc::new(self.gid.finish())
    }
    pub fn finish_mtime(&mut self) -> Arc<dyn Array> {
        Arc::new(self.mtime.finish())
    }
}

#[cfg(unix)]
pub fn lines2batch_iter<I>(
    lines: I,
    schema: SchemaRef,
    batch_size: usize,
) -> Result<impl Iterator<Item = Result<RecordBatch, io::Error>>, io::Error>
where
    I: Iterator<Item = Result<String, io::Error>>,
{
    Ok(Lines2BatchIter {
        lines,
        schema,
        batch_size,
        bldr: Builder {
            path: StringBuilder::new(),
            file_type: StringBuilder::new(),
            read_only: BooleanBuilder::new(),
            mode: UInt32Builder::new(),
            nlink: UInt64Builder::new(),
            len: UInt64Builder::new(),
            uid: UInt32Builder::new(),
            gid: UInt32Builder::new(),
            mtime: TimestampSecondBuilder::new(),
        },
    })
}

#[cfg(unix)]
struct Lines2BatchIter<I> {
    lines: I,
    schema: SchemaRef,
    batch_size: usize,
    bldr: Builder,
}

#[cfg(unix)]
impl<I> Iterator for Lines2BatchIter<I>
where
    I: Iterator<Item = Result<String, io::Error>>,
{
    type Item = Result<RecordBatch, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut taken = (&mut self.lines).take(self.batch_size);

        let robat = lines2batch(&mut taken, self.schema.clone(), &mut self.bldr);

        match robat {
            Err(e) => Some(Err(e)),
            Ok(None) => None,
            Ok(Some(r)) => Some(Ok(r)),
        }
    }
}
