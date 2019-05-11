use memmap::{Mmap, MmapMut};

use std::{
	fs::{File, OpenOptions},
	io::{self, Cursor, Write},
	ops::{Deref, DerefMut},
	path::Path,
};

pub type MmapFile = MmappedFile<Mmap>;
pub type MmapMutFile = MmappedFile<MmapMut>;

pub struct MmappedFile<M>
where
	M: AsRef<[u8]> + Deref<Target = [u8]>,
{
	file: File,
	map: M,
}

impl<M> MmappedFile<M>
where
	M: AsRef<[u8]> + Deref<Target = [u8]>,
{
	pub fn len(&self) -> io::Result<u64> {
		Ok(self.file.metadata()?.len())
	}

	pub fn is_empty(&self) -> io::Result<bool> {
		Ok(self.len()? == 0)
	}
}

impl MmapFile {
	pub unsafe fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
		log::info!("Opening memory mapped file '{}'", path.as_ref().display());

		let file = File::open(&path)?;
		let map = Mmap::map(&file)?;

		log::debug!("mmapped file open successful");

		Ok(Self { file, map })
	}

	pub unsafe fn as_str_unchecked(&self) -> &str {
		std::str::from_utf8_unchecked(self.deref())
	}
}

impl MmapMutFile {
	pub unsafe fn create_with_size(path: &Path, size: usize) -> io::Result<Self> {
		log::info!("Creating memory mapped file '{}'", path.display());

		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(&path)?;

		file.set_len(size as _)?;
		let map = MmapMut::map_mut(&file)?;

		log::debug!("mmapped file creation successful, size '{}'", size);

		Ok(Self { file, map })
	}

	pub unsafe fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
		const DEFAULT_SZ: usize = 8192;

		Self::create_with_size(path.as_ref(), DEFAULT_SZ)
	}

	fn resize(&mut self, new_len: u64) -> io::Result<()> {
		self.file.set_len(new_len)?;
		self.map = unsafe { MmapMut::map_mut(&self.file) }?;
		Ok(())
	}

	pub fn into_writer(self) -> MmappedWriter {
		let inner = self;
		let pos = 0;

		MmappedWriter { inner, pos }
	}
}

pub struct MmappedWriter {
	inner: MmappedFile<MmapMut>,
	pos: usize,
}

impl MmappedWriter {
	fn resize(&mut self, new_len: usize) -> io::Result<()> {
		log::trace!("resizing mmapped file to {} bytes", new_len);

		// resize underlying file and reset mmap
		self.inner.resize(new_len as _)?;

		// reset pos to original, or eof
		self.pos = std::cmp::min(self.pos, new_len);

		Ok(())
	}

	fn generate_cursor(&mut self) -> Cursor<&mut [u8]> {
		let mut cursor = Cursor::new(&mut *self.inner.map);
		cursor.set_position(self.pos as _);
		cursor
	}
}

impl Write for MmappedWriter {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {

		while let Err(e) = self.generate_cursor().write_all(&buf) {
			if e.kind() == io::ErrorKind::WriteZero {
				self.resize(2 * self.inner.map.len())?;
			} else {
				return Err(e);
			}
		}

		self.pos += buf.len();

		Ok(buf.len())
	}

	fn flush(&mut self) -> io::Result<()> {
		self.inner.map.flush_async()
	}

	fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
		// write already forces a write_all
		let _ = self.write(&buf)?;
		Ok(())
	}
}

impl Drop for MmappedWriter {
	fn drop(&mut self) {
		if let Err(e) = self.inner.resize(self.pos as _) {
			log::error!("error when dropping MmappedWriter '{}'", e)
		} else {
			log::trace!(
				"dropping mmapped file writer, truncating to '{}' bytes",
				self.pos
			);
		}

		if let Err(e) = self.flush() {
			log::error!("{}", e);
		}
	}
}

impl<M> Deref for MmappedFile<M>
where
	M: AsRef<[u8]> + Deref<Target = [u8]>,
{
	type Target = M::Target;

	fn deref(&self) -> &Self::Target {
		self.map.deref()
	}
}

impl<M> AsRef<[u8]> for MmappedFile<M>
where
	M: AsRef<[u8]> + Deref<Target = [u8]>,
{
	fn as_ref(&self) -> &[u8] {
		self.map.as_ref()
	}
}

impl DerefMut for MmappedFile<MmapMut> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.map.deref_mut()
	}
}

impl AsMut<[u8]> for MmappedFile<MmapMut> {
	fn as_mut(&mut self) -> &mut [u8] {
		self.map.as_mut()
	}
}
