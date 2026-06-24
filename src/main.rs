use anyhow::{Context, Result};
use clap::Parser;
use colored::*;
use dashmap::DashMap;
use filetime::FileTime;
use ignore::WalkBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const STATE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("sync_state");

#[derive(Parser, Debug)]
#[command(
    name = "syncron",
    version,
    about = "A bidirectional sync tool based on heuristics.",
    long_about = "A synchronization tool that uses a state database to track deletions and heuristics to skip large files with insignificant timestamp jitter."
)]
struct Args {
    /// Path to the first vault.
    #[arg(value_name = "PATH_1")]
    vault_1: Option<String>,

    /// Path to the second vault.
    #[arg(value_name = "PATH_2")]
    vault_2: Option<String>,

    /// Trial run listing changes without modifying files or the database.
    #[arg(short, long)]
    dry_run: bool,

    /// Show the physical locations of the state database and path logs.
    #[arg(short, long)]
    config: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct LastPaths {
    vault_1: PathBuf,
    vault_2: PathBuf,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct FileMeta {
    size: u64,
    seconds: i64,
    nanos: u32,
}

struct SyncStats {
    copied: std::sync::atomic::AtomicUsize,
    updated: std::sync::atomic::AtomicUsize,
    deleted: std::sync::atomic::AtomicUsize,
    errors: std::sync::atomic::AtomicUsize,
}

struct SyncContext<'a, 'txn> {
    arguments: &'a Args,
    statistics: &'a SyncStats,
    state_table: &'a mut redb::Table<'txn, &'static str, &'static [u8]>,
}

fn main() -> Result<()> {
    let arguments = Args::parse();
    let configuration_directory = dirs::config_dir()
        .context("Could not locate configuration directory")?
        .join("syncron");
    fs::create_dir_all(&configuration_directory)?;

    let database_path = configuration_directory.join("state.redb");
    let log_path = configuration_directory.join("last_paths.bin");

    if arguments.config {
        println!("{}: {}", "State database".cyan(), database_path.display());
        println!("{}: {}", "Paths log".cyan(), log_path.display());
        return Ok(());
    }

    let (vault_1_path, vault_2_path) = resolve_vault_paths(&arguments, &log_path)?;
    println!(
        "{} {} ↔ {}",
        "Syncing:".bold(),
        vault_1_path.display().to_string().green(),
        vault_2_path.display().to_string().blue()
    );

    let (metadata_1, metadata_2) = rayon::join(
        || scan_directory(&vault_1_path),
        || scan_directory(&vault_2_path),
    );

    let database = Database::create(&database_path)?;

    let statistics = SyncStats {
        copied: 0.into(),
        updated: 0.into(),
        deleted: 0.into(),
        errors: 0.into(),
    };

    run_sync_engine(
        &vault_1_path,
        &vault_2_path,
        metadata_1,
        metadata_2,
        &database,
        &arguments,
        &statistics,
    )?;

    print_summary(&statistics, arguments.dry_run);

    Ok(())
}

fn scan_directory(root: &Path) -> Arc<DashMap<String, FileMeta>> {
    let metadata_map = Arc::new(DashMap::new());
    let absolute_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());

    let walker = WalkBuilder::new(&absolute_root)
        .threads(num_cpus::get())
        .hidden(false)
        .build_parallel();

    walker.run(|| {
        let map_reference = metadata_map.clone();
        let root_reference = absolute_root.clone();
        Box::new(move |entry_result| {
            if let Ok(entry) = entry_result
                && entry.file_type().is_some_and(|kind| kind.is_file())
                && let (Ok(rel_path), Ok(sys_meta)) =
                    (entry.path().strip_prefix(&root_reference), entry.metadata())
            {
                let ft = FileTime::from_last_modification_time(&sys_meta);
                map_reference.insert(
                    rel_path.to_string_lossy().to_string(),
                    FileMeta {
                        size: sys_meta.len(),
                        seconds: ft.unix_seconds(),
                        nanos: ft.nanoseconds(),
                    },
                );
            }
            ignore::WalkState::Continue
        })
    });
    metadata_map
}

fn run_sync_engine(
    vault_1: &Path,
    vault_2: &Path,
    meta_1: Arc<DashMap<String, FileMeta>>,
    meta_2: Arc<DashMap<String, FileMeta>>,
    database: &Database,
    arguments: &Args,
    statistics: &SyncStats,
) -> Result<()> {
    let mut combined_paths: HashSet<String> =
        meta_1.iter().map(|entry| entry.key().clone()).collect();
    for entry in meta_2.iter() {
        combined_paths.insert(entry.key().clone());
    }

    let progress_bar = ProgressBar::new(combined_paths.len() as u64);
    progress_bar.set_style(ProgressStyle::default_bar().template(
        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
    )?);

    let write_transaction = database.begin_write()?;
    {
        let mut state_table = write_transaction.open_table(STATE_TABLE)?;

        let mut paths_to_forget = Vec::new();
        for item_result in state_table.iter()? {
            let (key_handle, _val_handle) = item_result?;
            let stored_relative_path = key_handle.value();

            let present_in_1 = meta_1.contains_key(stored_relative_path);
            let present_in_2 = meta_2.contains_key(stored_relative_path);

            if !present_in_1 && present_in_2 {
                let target_path = vault_2.join(stored_relative_path);
                if !arguments.dry_run && target_path.exists() {
                    let _ = fs::remove_file(target_path);
                }
                statistics
                    .deleted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                paths_to_forget.push(stored_relative_path.to_string());
            } else if present_in_1 && !present_in_2 {
                let target_path = vault_1.join(stored_relative_path);
                if !arguments.dry_run && target_path.exists() {
                    let _ = fs::remove_file(target_path);
                }
                statistics
                    .deleted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                paths_to_forget.push(stored_relative_path.to_string());
            }
        }

        for path_string in paths_to_forget {
            state_table.remove(path_string.as_str())?;
        }

        let mut context = SyncContext {
            arguments,
            statistics,
            state_table: &mut state_table,
        };

        for relative_path in combined_paths {
            let info_1 = meta_1.get(&relative_path);
            let info_2 = meta_2.get(&relative_path);

            match (info_1, info_2) {
                (Some(data_1), None) => {
                    sync_file(
                        &vault_1.join(&relative_path),
                        &vault_2.join(&relative_path),
                        &mut context,
                        &relative_path,
                        &data_1,
                        "COPY",
                    )?;
                }
                (None, Some(data_2)) => {
                    sync_file(
                        &vault_2.join(&relative_path),
                        &vault_1.join(&relative_path),
                        &mut context,
                        &relative_path,
                        &data_2,
                        "COPY",
                    )?;
                }
                (Some(data_1), Some(data_2)) => {
                    let time_diff_secs = (data_1.seconds - data_2.seconds).abs();
                    let size_diff = (data_1.size as i64 - data_2.size as i64).unsigned_abs();

                    let size_threshold = 100 * 1024;
                    let size_tolerance = 1024;
                    let max_size = std::cmp::max(data_1.size, data_2.size);

                    if time_diff_secs >= 1 {
                        let is_jitter = max_size > size_threshold && size_diff <= size_tolerance;

                        if !is_jitter {
                            if data_1.seconds > data_2.seconds {
                                sync_file(
                                    &vault_1.join(&relative_path),
                                    &vault_2.join(&relative_path),
                                    &mut context,
                                    &relative_path,
                                    &data_1,
                                    "UPDATE",
                                )?;
                            } else if data_2.seconds > data_1.seconds {
                                sync_file(
                                    &vault_2.join(&relative_path),
                                    &vault_1.join(&relative_path),
                                    &mut context,
                                    &relative_path,
                                    &data_2,
                                    "UPDATE",
                                )?;
                            }
                        }
                    }
                }
                _ => {}
            }
            progress_bar.inc(1);
        }
    }

    if !arguments.dry_run {
        prune_empty_directories(vault_1, statistics)?;
        prune_empty_directories(vault_2, statistics)?;
        write_transaction.commit()?;
    }
    progress_bar.finish_and_clear();
    Ok(())
}

fn prune_empty_directories(root: &Path, statistics: &SyncStats) -> Result<()> {
    if !root.is_dir() {
        return Ok(());
    }

    let entries = match fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => {
            statistics.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let _ = prune_empty_directories(&path, statistics);
        }
    }

    if let Ok(mut entries) = fs::read_dir(root)
        && entries.next().is_none() && root.parent().is_some() {
            if fs::remove_dir(root).is_err() {
                statistics.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                statistics.deleted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    Ok(())
}

fn sync_file(
    source: &Path,
    destination: &Path,
    context: &mut SyncContext,
    relative_path: &str,
    metadata_record: &FileMeta,
    action_type: &str,
) -> Result<()> {
    if !context.arguments.dry_run {
        if let Some(parent_path) = destination.parent() {
            fs::create_dir_all(parent_path)?;
        }
        if fs::copy(source, destination).is_err() {
            context
                .statistics
                .errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }
        let binary_blob = postcard::to_stdvec(metadata_record)?;
        context
            .state_table
            .insert(relative_path, binary_blob.as_slice())?;
    }

    if action_type == "COPY" {
        context
            .statistics
            .copied
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    } else {
        context
            .statistics
            .updated
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}

fn resolve_vault_paths(arguments: &Args, log_path: &Path) -> Result<(PathBuf, PathBuf)> {
    let mut vault_1_final = arguments.vault_1.clone().map(PathBuf::from);
    let mut vault_2_final = arguments.vault_2.clone().map(PathBuf::from);

    if (vault_1_final.is_none() || vault_2_final.is_none())
        && let Ok(serialized_data) = fs::read(log_path)
    {
        let cached_paths: LastPaths = postcard::from_bytes(&serialized_data)?;
        vault_1_final = vault_1_final.or(Some(cached_paths.vault_1));
        vault_2_final = vault_2_final.or(Some(cached_paths.vault_2));
    }

    let (path_1, path_2) = (
        vault_1_final.context("Vault 1 missing. Please provide a path.")?,
        vault_2_final.context("Vault 2 missing. Please provide a path.")?,
    );

    let path_1 = path_1.canonicalize().unwrap_or(path_1);
    let path_2 = path_2.canonicalize().unwrap_or(path_2);

    let persistent_config = postcard::to_stdvec(&LastPaths {
        vault_1: path_1.clone(),
        vault_2: path_2.clone(),
    })?;
    fs::write(log_path, persistent_config)?;

    Ok((path_1, path_2))
}

fn print_summary(statistics: &SyncStats, is_dry_run: bool) {
    let copied_total = statistics.copied.load(std::sync::atomic::Ordering::Relaxed);
    let updated_total = statistics
        .updated
        .load(std::sync::atomic::Ordering::Relaxed);
    let deleted_total = statistics
        .deleted
        .load(std::sync::atomic::Ordering::Relaxed);
    let error_total = statistics.errors.load(std::sync::atomic::Ordering::Relaxed);

    if is_dry_run {
        println!("\n{}", "Dry Run Summary:".bold().yellow());
    } else {
        println!("\n{}", "Summary:".bold().underline());
    }

    if copied_total > 0 {
        println!("{} {}", "COPY:".green(), copied_total);
    }
    if updated_total > 0 {
        println!("{} {}", "UPDATE:".blue(), updated_total);
    }
    if deleted_total > 0 {
        println!("{} {}", "DELETE:".yellow(), deleted_total);
    }
    if error_total > 0 {
        println!("{} {}", "ERRORS:".red(), error_total);
    }
}
