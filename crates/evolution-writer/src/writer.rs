use evolution_common::error::{Result, SetupError};

use std::fs::{File, OpenOptions};
use std::path::PathBuf;

///
pub trait Writer {}
///
pub type WriterRef = Box<dyn Writer>;

/// The writer struct for fixed-length files (.flf).
pub struct FixedLengthFileWriter {
    /// The file descriptor that the writer writes the data to.
    inner: File,
    /// The properties of the opened file descriptor.
    properties: FixedLengthFileWriterProperties,
}

impl FixedLengthFileWriter {
    /// Create a new instance of a [`FixedLengthFileWriterBuilder`] with default values.
    pub fn builder() -> FixedLengthFileWriterBuilder {
        FixedLengthFileWriterBuilder {
            ..Default::default()
        }
    }
}

impl Writer for FixedLengthFileWriter {}

/// A collection of various file properties for the [`FixedLengthFileWriter`].
pub struct FixedLengthFileWriterProperties {
    /// Enforce that a file with the same name can not already exist. If it already exists,
    /// the [`FixedLengthFileWriterBuilder`] will return a [`SetupError`].
    force_create_new: bool,
    /// Allow file with same name to already exist, in such a case, the file will be opened.
    create_or_open: bool,
    /// Set the option to truncate the file if it already exists. This will set the length
    /// of the file to 0. If the file does not already exist, this has no effect.
    truncate_existing: bool,
}

impl FixedLengthFileWriterProperties {
    /// Create a new instance of a [`FixedLengthFileWriterPropertiesBuilder`] with default values.
    pub fn builder() -> FixedLengthFileWriterPropertiesBuilder {
        FixedLengthFileWriterPropertiesBuilder {
            ..Default::default()
        }
    }
}

/// A helper struct for building an instance of a [`FixedLengthFileWriter`] struct.
#[derive(Default)]
pub struct FixedLengthFileWriterBuilder {
    /// The path to open a new file descriptor at.
    out_path: Option<PathBuf>,
    /// The properties of the file descriptor.
    properties: Option<FixedLengthFileWriterProperties>,
}

impl FixedLengthFileWriterBuilder {
    /// Set the relative or absolute path to write the fixed-length file (.flf) to.
    pub fn with_out_path(mut self, out_path: PathBuf) -> Self {
        self.out_path = Some(out_path);
        self
    }

    /// Set the properties to use when opening the file descriptor.
    pub fn with_properties(mut self, properties: FixedLengthFileWriterProperties) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Try creating a new [`FixedLengthFileWriter`] from the previously set values.
    ///
    /// # Errors
    /// If any of the required fields have not been set and thus are `None`, or if
    /// the properties contained an invalid combination of settings. See [`OpenOptions`]
    /// documentation for details on all errors it can return.
    pub fn try_build(self) -> Result<FixedLengthFileWriter> {
        let out_path: PathBuf = self.out_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'out_path' was not provided, exiting...",
            ))
        })?;

        let properties: FixedLengthFileWriterProperties = self.properties.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'properties' was not provided, exiting...",
            ))
        })?;

        let inner: File = OpenOptions::new()
            .write(true)
            .create_new(properties.force_create_new)
            .create(properties.create_or_open)
            .append(properties.create_or_open && !properties.truncate_existing)
            .truncate(properties.truncate_existing && !properties.force_create_new)
            .open(out_path)?;

        Ok(FixedLengthFileWriter { inner, properties })
    }

    /// Creates a new [`FixedLengthFileWriter`] from the previously set values.
    ///
    /// # Note
    /// This method internally calls the [`try_build`] method and simply unwraps the returned
    /// [`Result`]. If you don't care about error propagation, use this method over [`try_build`].
    ///
    /// # Panics
    /// If any of the required fields have not been set and thus are `None`, or if the
    /// properties contained an invalid combination of settings.
    ///
    /// [`try_build`]: FixedLengthFileWriterBuilder::try_build
    pub fn build(self) -> FixedLengthFileWriter {
        self.try_build().unwrap()
    }
}

/// A helper struct for building an instance of a [`FixedLengthFileWriterProperties`] struct.
#[derive(Default)]
pub struct FixedLengthFileWriterPropertiesBuilder {
    force_create_new: Option<bool>,
    create_or_open: Option<bool>,
    truncate_existing: Option<bool>,
}

impl FixedLengthFileWriterPropertiesBuilder {
    /// Set the option to enforce that there can not already exist a file with the same name.
    pub fn with_force_create_new(mut self, force_create_new: bool) -> Self {
        self.force_create_new = Some(force_create_new);
        self
    }

    /// Set the option to allow there existing a file with the same name, in which case,
    /// the file will be opened and appended to.
    pub fn with_create_or_open(mut self, create_or_open: bool) -> Self {
        self.create_or_open = Some(create_or_open);
        self
    }

    /// Set the option to truncate the file if it already exists. If the file does not already
    /// exist, then this option has no effect.
    pub fn with_truncate_existing(mut self, truncate_existing: bool) -> Self {
        self.truncate_existing = Some(truncate_existing);
        self
    }

    /// Try creating a new [`FixedLengthFileWriterProperties`] from the previously set values.
    ///
    /// # Errors
    /// Iff any of the required fields have not been set and thus are `None`.
    pub fn try_build(self) -> Result<FixedLengthFileWriterProperties> {
        let force_create_new: bool = self.force_create_new.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'force_create_new' was not provided, exiting...",
            ))
        })?;

        let create_or_open: bool = self.create_or_open.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'create_or_open' was not provided, exiting...",
            ))
        })?;

        let truncate_existing: bool = self.truncate_existing.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'truncate_existing' was not provided, exiting...",
            ))
        })?;

        Ok(FixedLengthFileWriterProperties {
            force_create_new,
            create_or_open,
            truncate_existing,
        })
    }

    /// Creates a new [`FixedLengthFileWriterProperties`] from the previously set values.
    ///
    /// # Note
    /// This method internally calls the [`try_build`] method and simply unwraps the returned
    /// [`Result`]. If you don't care about error propagation, use this method over [`try_build`].
    ///
    /// # Panics
    /// Iff any of the required fields have not been set and thus are `None`.
    ///
    /// [`try_build`]: FixedLengthFileWriterPropertiesBuilder::try_build
    pub fn build(self) -> FixedLengthFileWriterProperties {
        self.try_build().unwrap()
    }
}
