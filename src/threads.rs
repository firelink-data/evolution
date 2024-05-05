use log::{info, warn};

pub(crate) fn get_available_threads(n_wanted_threads: usize) -> usize {
    let n_logical_threads: usize = num_cpus::get();

    if n_wanted_threads > n_logical_threads {
        warn!(
            "You specified to use {} threads, but your CPU only has {} logical threads.",
            n_wanted_threads, n_logical_threads,
        );
        info!(
            "Will use all available logical threads ({}).",
            n_logical_threads,
        );
        return n_logical_threads;
    };

    info!("Executing using {} logical threads.", n_wanted_threads);

    n_wanted_threads
}
