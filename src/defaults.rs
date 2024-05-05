
// This default value should depend on the memory capacity of the system
// running the program. Because the workers produce buffers faster than
// the master can write them to disk we need to bound the worker/master
// channel. If you have a lot of system memory, you can increase this value,
// but if you increase it too much the program will run out of system memory.
pub(crate) static DEFAULT_THREAD_CHANNEL_CAPACITY: usize = 128;
pub(crate) static DEFAULT_MIN_N_ROWS_FOR_MULTITHREADING: usize = 1000;
pub(crate) static DEFAULT_MOCKED_FILENAME_LEN: usize = 8;
pub(crate) static DEFAULT_ROW_BUFFER_LEN: usize = 1024;

#[cfg(target_os = "windows")]
static NUM_CHARS_FOR_NEWLINE: usize = 2;
#[cfg(not(target_os = "windows"))]
static NUM_CHARS_FOR_NEWLINE: usize = 1;

