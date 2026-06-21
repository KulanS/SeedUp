"""
SeedUp - Smart Torrent Management Tool
Incremental download-upload pipeline with two threads.

Copyright 2025 Ishara Deshapriya

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Two-thread pipeline:
  Thread 1 (Downloader): Runs libtorrent, detects per-file completion, queues files.
  Thread 2 (Uploader):   Uploads completed files to GDrive, verifies, deletes local copies.
"""

import libtorrent as lt
import threading
import queue
import time
import os
import sys
import shutil
from dataclasses import dataclass, field
from typing import List, Optional

from config import (
    TORRENT_SESSION_FILE, TORRENT_DOWNLOAD_PATH, PIPELINE_QUEUE_SIZE,
    DRIVE_SPACE_BUFFER, UPLOAD_MAX_RETRIES, UPLOAD_RETRY_DELAY,
    DISK_SAFETY_THRESHOLD, get_logger
)
from torrent_downloader import save_session, load_session
from torrent_inspector import format_size

logger = get_logger(__name__)


# ─── Data Classes ────────────────────────────────────────────────────────────

@dataclass
class FileCompleteInfo:
    """Information about a completed file ready for upload."""
    file_index: int         # libtorrent file index
    path: str               # relative path within torrent
    local_path: str         # absolute local path on disk
    size: int               # file size in bytes
    name: str               # filename (basename)


@dataclass
class PipelineResult:
    """Summary of the pipeline execution."""
    torrent_name: str
    total_size: int
    files_downloaded: int
    files_uploaded: int
    files_failed: list = field(default_factory=list)
    files_skipped: list = field(default_factory=list)
    elapsed_time: float = 0.0
    drive_folder_id: str = ""
    success: bool = True


# ─── Shared Progress State ───────────────────────────────────────────────────

class PipelineProgress:
    """Thread-safe shared progress state for the pipeline."""

    def __init__(self, total_size, total_files):
        self._lock = threading.Lock()
        self.total_size = total_size
        self.total_files = total_files

        # Download state
        self.download_progress = 0.0       # 0.0 to 1.0
        self.download_speed = 0            # bytes/sec
        self.download_eta = "N/A"
        self.download_done = False
        self.seeds = 0
        self.peers = 0

        # Upload state
        self.uploaded_size = 0             # bytes
        self.uploaded_files = 0
        self.upload_queue_size = 0
        self.upload_done = False

        # Header lines to preserve when clearing output
        self._header_lines = []

        # Detect IPython/Colab environment
        self._use_clear_output = False
        self._clear_output_fn = None
        try:
            from IPython.display import clear_output
            self._clear_output_fn = clear_output
            self._use_clear_output = True
        except ImportError:
            pass

    def set_header(self, lines):
        """Store header lines to reprint after each clear_output."""
        self._header_lines = lines

    def update_download(self, progress, speed, eta, seeds, peers):
        with self._lock:
            self.download_progress = progress
            self.download_speed = speed
            self.download_eta = eta
            self.seeds = seeds
            self.peers = peers

    def update_upload(self, uploaded_size, uploaded_files, queue_size):
        with self._lock:
            self.uploaded_size = uploaded_size
            self.uploaded_files = uploaded_files
            self.upload_queue_size = queue_size

    def display(self):
        """Print the dual-progress display (Colab-compatible)."""
        with self._lock:
            # Download line
            dl_pct = self.download_progress * 100
            dl_bar_len = 30
            dl_filled = int(dl_bar_len * self.download_progress)
            dl_bar = '█' * dl_filled + '░' * (dl_bar_len - dl_filled)

            if self.download_speed > 1024 * 1024:
                speed_str = f"{self.download_speed / (1024 * 1024):.1f} MB/s"
            elif self.download_speed > 0:
                speed_str = f"{self.download_speed / 1024:.1f} KB/s"
            else:
                speed_str = "0 KB/s"

            dl_line = (f"Download: {dl_bar} {dl_pct:5.1f}%  | "
                       f"Speed: {speed_str} | ETA: {self.download_eta} | "
                       f"S:{self.seeds} P:{self.peers}")

            # Upload line
            if self.total_size > 0:
                ul_pct = (self.uploaded_size / self.total_size) * 100
            else:
                ul_pct = 0
            ul_bar_len = 30
            ul_filled = int(ul_bar_len * ul_pct / 100)
            ul_bar = '█' * ul_filled + '░' * (ul_bar_len - ul_filled)

            ul_line = (f"Upload:   {ul_bar} {ul_pct:5.1f}%  | "
                       f"{format_size(self.uploaded_size)}/{format_size(self.total_size)} | "
                       f"Files: {self.uploaded_files}/{self.total_files} | "
                       f"Queued: {self.upload_queue_size}")

        if self._use_clear_output:
            # Colab/IPython: clear cell output and reprint everything
            self._clear_output_fn(wait=True)
            for line in self._header_lines:
                print(line)
            print(dl_line)
            print(ul_line)
        else:
            # Terminal fallback: compact single-line with \r
            compact = (f"\rDL: {dl_pct:5.1f}% {speed_str} ETA:{self.download_eta} | "
                       f"UL: {ul_pct:5.1f}% {self.uploaded_files}/{self.total_files} files")
            print(compact, end="", flush=True)


# ─── Downloader Thread ────────────────────────────────────────────────────────

def _downloader_thread(source, download_path, session_file, auto_resume,
                       selected_indices, file_queue, progress, stop_event,
                       result_holder):
    """
    Downloader thread: runs libtorrent and queues completed files.

    :param source: torrent source (magnet or .torrent path)
    :param download_path: local download directory
    :param session_file: session state file path
    :param auto_resume: whether to resume from saved session
    :param selected_indices: list of file indices to download (None for all)
    :param file_queue: thread-safe queue to put completed files into
    :param progress: PipelineProgress object for progress reporting
    :param stop_event: threading.Event to signal stop on error
    :param result_holder: dict to store results from this thread
    """
    try:
        if not os.path.exists(download_path):
            os.makedirs(download_path)

        is_resuming = auto_resume and os.path.exists(session_file)
        ses = load_session(session_file) if auto_resume else lt.session()

        settings = {'listen_interfaces': '0.0.0.0:6881'}
        ses.apply_settings(settings)

        params = lt.add_torrent_params()
        params.save_path = download_path
        params.storage_mode = lt.storage_mode_t.storage_mode_sparse

        if source.startswith("magnet:"):
            params.url = source
        elif source.endswith(".torrent"):
            if not os.path.exists(source):
                result_holder['error'] = f"Torrent file not found: {source}"
                file_queue.put(None)
                return
            with open(source, "rb") as f:
                torrent_data = lt.bdecode(f.read())
                info = lt.torrent_info(torrent_data)
                params.ti = info
        else:
            result_holder['error'] = "Invalid source. Provide a .torrent file or magnet link."
            file_queue.put(None)
            return

        handle = ses.add_torrent(params)

        # Wait for metadata
        while not handle.status().has_metadata:
            if stop_event.is_set():
                file_queue.put(None)
                return
            time.sleep(1)

        torrent_info_obj = handle.get_torrent_info()
        file_storage = torrent_info_obj.files()
        num_files = file_storage.num_files()
        torrent_name = handle.status().name

        result_holder['torrent_name'] = torrent_name
        result_holder['total_size'] = torrent_info_obj.total_size()

        # Apply selective file priorities
        active_file_indices = set(range(num_files))
        if selected_indices is not None:
            priorities = [0] * num_files
            for idx in selected_indices:
                if 0 <= idx < num_files:
                    priorities[idx] = 1
            handle.prioritize_files(priorities)
            active_file_indices = set(idx for idx in selected_indices if 0 <= idx < num_files)
            logger.info(f"Selective download: {len(active_file_indices)}/{num_files} files")

        # Track per-file completion
        file_completed = set()
        files_downloaded = 0

        while handle.status().state != lt.torrent_status.seeding:
            if stop_event.is_set():
                save_session(ses, session_file)
                file_queue.put(None)
                return

            s = handle.status()

            # Calculate ETA
            eta_str = "N/A"
            if s.download_rate > 0:
                remaining = s.total_wanted - s.total_done
                eta_seconds = remaining / s.download_rate
                if eta_seconds < 60:
                    eta_str = f"{int(eta_seconds)}s"
                elif eta_seconds < 3600:
                    eta_str = f"{int(eta_seconds / 60)}m {int(eta_seconds % 60)}s"
                else:
                    hours = int(eta_seconds / 3600)
                    minutes = int((eta_seconds % 3600) / 60)
                    eta_str = f"{hours}h {minutes}m"

            progress.update_download(
                s.progress, s.download_rate, eta_str, s.num_seeds,
                s.num_peers - s.num_seeds
            )

            # Check for per-file completion
            file_progress = handle.file_progress()
            for i in active_file_indices:
                if i not in file_completed:
                    expected_size = file_storage.file_size(i)
                    if expected_size > 0 and file_progress[i] >= expected_size:
                        file_completed.add(i)
                        files_downloaded += 1

                        file_path = file_storage.file_path(i)
                        local_path = os.path.join(download_path, file_path)
                        file_name = os.path.basename(file_path)

                        file_info = FileCompleteInfo(
                            file_index=i,
                            path=file_path,
                            local_path=local_path,
                            size=expected_size,
                            name=file_name,
                        )
                        logger.info(f"File completed: {file_name} ({format_size(expected_size)})")
                        file_queue.put(file_info)

            # Disk pressure management
            try:
                free_disk = shutil.disk_usage(download_path).free
                if free_disk < DISK_SAFETY_THRESHOLD:
                    logger.warning("Low disk space — pausing download for uploader to catch up")
                    handle.pause()
                    while shutil.disk_usage(download_path).free < DISK_SAFETY_THRESHOLD * 2:
                        if stop_event.is_set():
                            break
                        time.sleep(5)
                    if not stop_event.is_set():
                        handle.resume()
                        logger.info("Disk space recovered — resuming download")
            except OSError:
                pass  # disk_usage can fail on some systems

            # Save session periodically
            if int(time.time()) % 10 == 0:
                save_session(ses, session_file)

            time.sleep(1)

        # Handle any remaining files that completed with the final seeding state
        file_progress = handle.file_progress()
        for i in active_file_indices:
            if i not in file_completed:
                expected_size = file_storage.file_size(i)
                if expected_size > 0 and file_progress[i] >= expected_size:
                    file_completed.add(i)
                    files_downloaded += 1
                    file_path = file_storage.file_path(i)
                    local_path = os.path.join(download_path, file_path)
                    file_name = os.path.basename(file_path)
                    file_info = FileCompleteInfo(
                        file_index=i, path=file_path, local_path=local_path,
                        size=expected_size, name=file_name,
                    )
                    file_queue.put(file_info)

        result_holder['files_downloaded'] = files_downloaded
        progress.download_done = True

        # Clean up session file on successful download
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
            except Exception:
                pass

    except KeyboardInterrupt:
        logger.warning("Download interrupted by user. Session saved.")
        result_holder['interrupted'] = True
    except Exception as e:
        logger.error(f"Downloader error: {e}")
        result_holder['error'] = str(e)
    finally:
        # Always signal uploader that no more files are coming
        file_queue.put(None)


# ─── Uploader Thread ─────────────────────────────────────────────────────────

def _uploader_thread(file_queue, folder_id, skip_existing, progress,
                     stop_event, result_holder):
    """
    Uploader thread: takes completed files from queue, uploads to GDrive,
    verifies, and deletes local copies.

    :param file_queue: thread-safe queue to get completed files from
    :param folder_id: GDrive folder ID for uploads
    :param skip_existing: whether to skip files already on Drive
    :param progress: PipelineProgress object for progress reporting
    :param stop_event: threading.Event to signal stop on error
    :param result_holder: dict to store results from this thread
    """
    uploaded_files = 0
    uploaded_size = 0
    failed_files = []
    skipped_files = []

    try:
        # Lazy import to avoid circular imports and allow non-Colab usage
        from gdrive_uploader import SimpleDriveUploader, get_or_create_seedup_folder

        uploader = SimpleDriveUploader(skip_existing=skip_existing, use_seedup_folder=(folder_id is None))

        if folder_id is None:
            folder_id = uploader.seedup_folder_id

        result_holder['drive_folder_id'] = folder_id or ""

        while True:
            try:
                file_info = file_queue.get(timeout=2)
            except queue.Empty:
                if stop_event.is_set():
                    break
                continue

            if file_info is None:
                # Sentinel: downloader is done
                break

            # Update queue size for progress
            progress.update_upload(uploaded_size, uploaded_files, file_queue.qsize())

            # Check if file already exists on Drive
            if skip_existing:
                existing = uploader.file_exists(file_info.name, folder_id)
                if existing:
                    existing_size = int(existing.get('size', 0))
                    if existing_size == file_info.size:
                        logger.info(f"Skipped (exists on Drive): {file_info.name}")
                        skipped_files.append(file_info.path)
                        uploaded_size += file_info.size
                        uploaded_files += 1
                        progress.update_upload(uploaded_size, uploaded_files, file_queue.qsize())

                        # Delete local file since it's already on Drive
                        _delete_local_file(file_info.local_path)
                        continue

            # Upload with retries
            upload_success = False
            for attempt in range(1, UPLOAD_MAX_RETRIES + 1):
                if stop_event.is_set():
                    break

                try:
                    # Ensure parent directories exist in Drive
                    target_folder_id = _ensure_drive_folders(
                        uploader, file_info.path, folder_id
                    )

                    file_id = uploader.upload_file(file_info.local_path, target_folder_id)
                    if file_id:
                        upload_success = True
                        break
                    else:
                        logger.warning(f"Upload attempt {attempt}/{UPLOAD_MAX_RETRIES} "
                                       f"failed for {file_info.name}")
                except Exception as e:
                    logger.warning(f"Upload attempt {attempt}/{UPLOAD_MAX_RETRIES} "
                                   f"error for {file_info.name}: {e}")

                if attempt < UPLOAD_MAX_RETRIES:
                    delay = UPLOAD_RETRY_DELAY * (2 ** (attempt - 1))
                    time.sleep(delay)

            if upload_success:
                uploaded_files += 1
                uploaded_size += file_info.size
                logger.info(f"Uploaded: {file_info.name} ({format_size(file_info.size)})")

                # Delete local file after successful upload
                _delete_local_file(file_info.local_path)
            else:
                failed_files.append(file_info.path)
                logger.error(f"Failed to upload after {UPLOAD_MAX_RETRIES} attempts: "
                             f"{file_info.name}")
                # Keep local file on failure

            progress.update_upload(uploaded_size, uploaded_files, file_queue.qsize())

    except Exception as e:
        logger.error(f"Uploader error: {e}")
        result_holder['upload_error'] = str(e)
    finally:
        result_holder['files_uploaded'] = uploaded_files
        result_holder['files_failed'] = failed_files
        result_holder['files_skipped'] = skipped_files
        progress.upload_done = True


def _ensure_drive_folders(uploader, file_path, root_folder_id):
    """
    Ensure the directory structure for a file exists on Google Drive.

    :param uploader: SimpleDriveUploader instance
    :param file_path: relative file path (e.g. "TorrentName/subdir/file.txt")
    :param root_folder_id: root folder ID on Drive
    :return: folder ID where the file should be uploaded
    """
    parts = file_path.replace("\\", "/").split("/")
    # Remove the filename (last part)
    dir_parts = parts[:-1]

    current_folder_id = root_folder_id
    for dir_name in dir_parts:
        if not dir_name:
            continue
        # Check if folder exists, create if not
        existing_id = uploader.folder_exists(dir_name, current_folder_id)
        if existing_id:
            current_folder_id = existing_id
        else:
            new_id = uploader.create_folder(dir_name, current_folder_id)
            if new_id:
                current_folder_id = new_id
            else:
                logger.error(f"Failed to create Drive folder: {dir_name}")
                return root_folder_id  # fallback to root

    return current_folder_id


def _delete_local_file(local_path):
    """Delete a local file and clean up empty parent directories."""
    try:
        if os.path.exists(local_path):
            os.remove(local_path)
            logger.debug(f"Deleted local file: {local_path}")

            # Clean up empty parent directories
            parent = os.path.dirname(local_path)
            while parent and os.path.isdir(parent):
                try:
                    if not os.listdir(parent):
                        os.rmdir(parent)
                        parent = os.path.dirname(parent)
                    else:
                        break
                except OSError:
                    break
    except Exception as e:
        logger.warning(f"Could not delete local file {local_path}: {e}")


# ─── Pipeline Orchestrator ───────────────────────────────────────────────────

def run_pipeline(source, download_path=TORRENT_DOWNLOAD_PATH,
                 session_file=TORRENT_SESSION_FILE, auto_resume=True,
                 selected_indices=None, folder_id=None,
                 skip_existing=True):
    """
    Run the incremental download → upload pipeline.

    Pre-flight checks GDrive capacity, then starts two threads:
    downloader and uploader, connected by a thread-safe queue.

    :param source: torrent source (magnet or .torrent path)
    :param download_path: local download directory
    :param session_file: session state file path
    :param auto_resume: whether to resume from saved session
    :param selected_indices: list of libtorrent file indices to download (None for all)
    :param folder_id: GDrive folder ID (None for auto SeedUp Downloads folder)
    :param skip_existing: skip files already on Drive
    :return: PipelineResult object
    """
    start_time = time.time()

    # We need metadata first to know total size for the space check.
    # Use torrent_inspector for this (non-destructive metadata fetch).
    from torrent_inspector import inspect_torrent
    from gdrive_uploader import validate_drive_capacity

    print("━" * 60)
    print("📦 SEEDUP PIPELINE — Incremental Download & Upload")
    print("━" * 60)

    # Fetch metadata
    print("\n📋 Fetching torrent metadata...")
    torrent_info = inspect_torrent(source)
    if torrent_info is None:
        return PipelineResult(
            torrent_name="Unknown", total_size=0,
            files_downloaded=0, files_uploaded=0,
            files_failed=["metadata_fetch_failed"], success=False,
            elapsed_time=time.time() - start_time,
        )

    # Calculate effective size based on selection
    if selected_indices is not None:
        effective_size = sum(
            f.size for f in torrent_info.all_files
            if f.file_index in set(selected_indices)
        )
        effective_files = len(selected_indices)
    else:
        effective_size = torrent_info.total_size
        effective_files = torrent_info.file_count

    print(f"   Torrent: {torrent_info.name}")
    print(f"   Size: {format_size(effective_size)} ({effective_files} files)")

    # Pre-flight: GDrive space check
    print("\n📊 Checking Google Drive space...")
    is_sufficient, space_info = validate_drive_capacity(effective_size)
    if not is_sufficient:
        print("❌ Exiting — not enough space on Google Drive.")
        sys.exit(1)

    if space_info:
        print(f"   ✅ Drive: {space_info['free_hr']} free / {space_info['total_hr']} total")

    # Set up pipeline
    file_queue = queue.Queue(maxsize=PIPELINE_QUEUE_SIZE)
    stop_event = threading.Event()

    progress = PipelineProgress(
        total_size=effective_size,
        total_files=effective_files,
    )

    # Store header lines so they're reprinted after each clear_output in Colab
    header_lines = [
        "━" * 60,
        "📦 SEEDUP PIPELINE — Incremental Download & Upload",
        "━" * 60,
        f"   Torrent: {torrent_info.name}",
        f"   Size: {format_size(effective_size)} ({effective_files} files)",
    ]
    if space_info:
        header_lines.append(f"   ✅ Drive: {space_info['free_hr']} free / {space_info['total_hr']} total")
    header_lines.append("")
    header_lines.append("🚀 Pipeline running...")
    header_lines.append("")
    progress.set_header(header_lines)

    dl_results = {}
    ul_results = {}

    # Start threads
    print(f"\n🚀 Starting pipeline...\n")

    dl_thread = threading.Thread(
        target=_downloader_thread,
        args=(source, download_path, session_file, auto_resume,
              selected_indices, file_queue, progress, stop_event, dl_results),
        daemon=True,
    )
    ul_thread = threading.Thread(
        target=_uploader_thread,
        args=(file_queue, folder_id, skip_existing, progress,
              stop_event, ul_results),
        daemon=True,
    )

    dl_thread.start()
    ul_thread.start()

    # Progress display loop
    try:
        while dl_thread.is_alive() or ul_thread.is_alive():
            progress.display()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user.")
        stop_event.set()
        # Wait for threads to finish gracefully
        dl_thread.join(timeout=10)
        ul_thread.join(timeout=30)

    # Wait for threads to complete
    dl_thread.join()
    ul_thread.join()

    # Final progress display
    progress.display()
    print()  # Clean newline after progress

    elapsed = time.time() - start_time

    # Build result
    result = PipelineResult(
        torrent_name=dl_results.get('torrent_name', torrent_info.name),
        total_size=effective_size,
        files_downloaded=dl_results.get('files_downloaded', 0),
        files_uploaded=ul_results.get('files_uploaded', 0),
        files_failed=ul_results.get('files_failed', []),
        files_skipped=ul_results.get('files_skipped', []),
        elapsed_time=elapsed,
        drive_folder_id=ul_results.get('drive_folder_id', ''),
        success=len(ul_results.get('files_failed', [])) == 0,
    )

    # Print summary
    _print_pipeline_summary(result)

    return result


def _print_pipeline_summary(result):
    """Print the final pipeline execution summary."""
    print("━" * 60)
    print("📋 PIPELINE SUMMARY")
    print("━" * 60)
    print(f"  Torrent:      {result.torrent_name}")
    print(f"  Total size:   {format_size(result.total_size)}")
    print(f"  Downloaded:   {result.files_downloaded} files")
    print(f"  Uploaded:     {result.files_uploaded} files")

    if result.files_skipped:
        print(f"  Skipped:      {len(result.files_skipped)} files (already on Drive)")

    if result.files_failed:
        print(f"  ❌ Failed:    {len(result.files_failed)} files")
        for f in result.files_failed:
            print(f"                - {f}")

    # Elapsed time
    if result.elapsed_time < 60:
        time_str = f"{result.elapsed_time:.0f}s"
    elif result.elapsed_time < 3600:
        mins = int(result.elapsed_time / 60)
        secs = int(result.elapsed_time % 60)
        time_str = f"{mins}m {secs}s"
    else:
        hours = int(result.elapsed_time / 3600)
        mins = int((result.elapsed_time % 3600) / 60)
        time_str = f"{hours}h {mins}m"

    print(f"  Elapsed:      {time_str}")

    if result.drive_folder_id:
        folder_url = f"https://drive.google.com/drive/folders/{result.drive_folder_id}"
        print(f"\n  📁 View on Drive: {folder_url}")

    if result.success:
        print(f"\n  🎉 All files processed successfully!")
    else:
        print(f"\n  ⚠️  Some files failed. Check logs above.")

    print("━" * 60)
