use std::io;

pub trait Fs {
    fn read(&mut self, name: &str) -> io::Result<Option<Vec<u8>>>;

    fn write(&mut self, name: &str, data: &[u8]) -> io::Result<()>;
    fn append(&mut self, name: &str, data: &[u8]) -> io::Result<()>;
    fn truncate(&mut self, name: &str, len: u64) -> io::Result<()>;

    fn rename(&mut self, from: &str, to: &str) -> io::Result<()>;
    fn remove(&mut self, name: &str) -> io::Result<()>;

    fn sync(&mut self, name: &str) -> io::Result<()>;
    fn sync_dir(&mut self) -> io::Result<()>;
}
