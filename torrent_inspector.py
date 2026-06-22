"""
SeedUp - Smart Torrent Management Tool
Torrent metadata inspector — fetches and displays torrent contents without downloading.

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
"""

import libtorrent as lt
import time
import os
import sys
from dataclasses import dataclass, field
from typing import List, Optional, Dict

from config import METADATA_TIMEOUT, TORRENT_DOWNLOAD_PATH, get_logger

logger = get_logger(__name__)


# ─── Data Classes ────────────────────────────────────────────────────────────

@dataclass
class TorrentFileInfo:
    """Information about a single file within the torrent."""
    file_index: int         # libtorrent internal file index
    path: str               # relative path within the torrent
    size: int               # size in bytes
    name: str               # filename (basename)


@dataclass
class TorrentGroup:
    """A first-level group (directory or standalone file) with an assigned index."""
    group_index: int                    # user-facing index for selection
    name: str                           # group name (folder name or filename)
    is_directory: bool                  # True if this is a folder group
    total_size: int                     # sum of all files in this group
    files: List[TorrentFileInfo]        # files in this group
    file_indices: List[int]             # libtorrent file indices in this group


@dataclass
class TorrentInfo:
    """Complete torrent metadata with grouped file tree."""
    name: str                           # torrent name
    info_hash: str                      # torrent info hash
    total_size: int                     # total size in bytes
    file_count: int                     # total number of files
    groups: List[TorrentGroup]          # first-level groups with indices
    all_files: List[TorrentFileInfo]    # flat list of all files
    is_single_file: bool                # True if torrent has exactly one file


# ─── Size Formatting ─────────────────────────────────────────────────────────

def format_size(size_bytes):
    """Format bytes into human-readable size string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


# ─── Metadata Fetching ───────────────────────────────────────────────────────

def inspect_torrent(source, timeout=METADATA_TIMEOUT):
    """
    Fetch torrent metadata without downloading any content.

    :param source: .torrent file path or magnet link.
    :param timeout: seconds to wait for metadata (magnets only).
    :return: TorrentInfo object, or None on failure.
    """
    ses = lt.session()
    settings = {'listen_interfaces': '0.0.0.0:6881'}
    ses.apply_settings(settings)

    params = lt.add_torrent_params()
    params.save_path = TORRENT_DOWNLOAD_PATH  # required but nothing will download
    params.storage_mode = lt.storage_mode_t.storage_mode_sparse

    # Handle magnet link or .torrent file
    if source.startswith("magnet:"):
        params.url = source
        logger.info(f"Fetching metadata for magnet link: {source[:60]}...")
    elif source.endswith(".torrent"):
        if not os.path.exists(source):
            logger.error(f"Torrent file not found: {source}")
            return None
        try:
            with open(source, "rb") as f:
                torrent_data = lt.bdecode(f.read())
                info = lt.torrent_info(torrent_data)
                params.ti = info
            logger.info(f"Reading torrent file: {source}")
        except Exception as e:
            logger.error(f"Failed to read torrent file: {e}")
            return None
    else:
        logger.error("Invalid source. Provide a .torrent file or magnet link.")
        return None

    # Set all file priorities to 0 (don't download anything)
    # For .torrent files we can set this before adding
    if params.ti:
        num_files = params.ti.files().num_files()
        params.file_priorities = [0] * num_files

    try:
        handle = ses.add_torrent(params)
    except Exception as e:
        logger.error(f"Failed to add torrent: {e}")
        return None

    # For magnets, set priorities to 0 after metadata arrives
    # Wait for metadata with a spinner
    if not handle.status().has_metadata:
        print("⏳ Fetching torrent metadata", end="", flush=True)
        start = time.time()
        while not handle.status().has_metadata:
            if time.time() - start > timeout:
                print()
                logger.error(f"Metadata fetch timed out after {timeout}s")
                ses.remove_torrent(handle)
                return None
            print(".", end="", flush=True)
            time.sleep(1)
        print(" ✓")

    # Set all priorities to 0 now that we have metadata (important for magnets)
    torrent_info = handle.get_torrent_info()
    num_files = torrent_info.files().num_files()
    handle.prioritize_files([0] * num_files)

    # Extract file information
    file_storage = torrent_info.files()
    all_files = []

    for i in range(num_files):
        file_path = file_storage.file_path(i)
        file_size = file_storage.file_size(i)
        file_name = os.path.basename(file_path)
        all_files.append(TorrentFileInfo(
            file_index=i,
            path=file_path,
            size=file_size,
            name=file_name,
        ))

    # Build first-level groups
    groups = _build_file_tree(all_files, torrent_info.name())

    info = TorrentInfo(
        name=torrent_info.name(),
        info_hash=str(torrent_info.info_hash()),
        total_size=torrent_info.total_size(),
        file_count=num_files,
        groups=groups,
        all_files=all_files,
        is_single_file=(num_files == 1),
    )

    # Clean up — remove the torrent from session (we only wanted metadata)
    ses.remove_torrent(handle)

    return info


# ─── File Tree Building ──────────────────────────────────────────────────────

def _build_file_tree(all_files, torrent_name):
    """
    Group files by their first-level directory within the torrent.

    Files directly in the torrent root get their own group.
    Files inside a subdirectory are grouped under that directory name.

    :param all_files: list of TorrentFileInfo objects
    :param torrent_name: name of the torrent (top-level directory)
    :return: list of TorrentGroup objects with assigned indices
    """
    # Group files by their first path component after the torrent name
    dir_groups = {}      # dirname -> list of TorrentFileInfo
    root_files = []      # files directly in the root

    for f in all_files:
        # Paths are relative, e.g. "TorrentName/subdir/file.txt" or "TorrentName/file.txt"
        parts = f.path.replace("\\", "/").split("/")

        # Remove the torrent name prefix if present
        if parts and parts[0] == torrent_name:
            parts = parts[1:]

        if len(parts) <= 1:
            # File is directly in the torrent root
            root_files.append(f)
        else:
            # File is inside a subdirectory — group by first component
            first_dir = parts[0]
            if first_dir not in dir_groups:
                dir_groups[first_dir] = []
            dir_groups[first_dir].append(f)

    # Assign group indices
    groups = []
    idx = 0

    # Directories first (sorted alphabetically)
    for dir_name in sorted(dir_groups.keys()):
        files = dir_groups[dir_name]
        total_size = sum(f.size for f in files)
        file_indices = [f.file_index for f in files]
        groups.append(TorrentGroup(
            group_index=idx,
            name=dir_name,
            is_directory=True,
            total_size=total_size,
            files=files,
            file_indices=file_indices,
        ))
        idx += 1

    # Then root-level files (sorted by name)
    for f in sorted(root_files, key=lambda x: x.name):
        groups.append(TorrentGroup(
            group_index=idx,
            name=f.name,
            is_directory=False,
            total_size=f.size,
            files=[f],
            file_indices=[f.file_index],
        ))
        idx += 1

    return groups


# ─── Display ─────────────────────────────────────────────────────────────────

def display_torrent_info(info, drive_status=None, drive_space=None):
    """
    Print a formatted table of the torrent contents.

    :param info: TorrentInfo object from inspect_torrent()
    :param drive_status: optional dict mapping file paths to status strings
                         (e.g. {"file.txt": "✅ Done", "big.iso": "⏳ Pending"})
    :param drive_space: optional dict with 'total', 'used', 'free' keys (bytes)
                        and 'total_hr', 'used_hr', 'free_hr' human-readable strings
    """
    width = 76

    # Header
    print(f"\n{'━' * width}")

    # Determine status label
    status_label = "New Download"
    if drive_status:
        done_count = sum(1 for v in drive_status.values() if "Done" in str(v))
        if done_count == len(drive_status) and done_count > 0:
            status_label = "✅ Fully Uploaded"
        elif done_count > 0:
            status_label = "⏳ Partially Uploaded"

    print(f"📦  Torrent: {info.name}")
    print(f"    Total Size: {format_size(info.total_size)}  |  "
          f"Files: {info.file_count}  |  Status: {status_label}")
    print(f"{'━' * width}")

    # Drive space info
    if drive_space:
        space_sufficient = drive_space.get('free', 0) > info.total_size
        space_icon = "✅" if space_sufficient else "❌"
        space_label = "Sufficient space" if space_sufficient else "INSUFFICIENT SPACE"
        print(f"\n  Google Drive: {drive_space.get('free_hr', 'N/A')} free / "
              f"{drive_space.get('total_hr', 'N/A')} total  {space_icon} {space_label}")

    # Overall GDrive transfer progress bar (when drive status is available)
    if drive_status and info.total_size > 0:
        done_size = 0
        done_files = 0
        for f in info.all_files:
            if f.path in drive_status and "Done" in drive_status[f.path]:
                done_size += f.size
                done_files += 1
        transfer_pct = (done_size / info.total_size) * 100
        bar_len = 40
        filled = int(bar_len * transfer_pct / 100)
        bar = '█' * filled + '░' * (bar_len - filled)

        print(f"\n  📊 GDrive Transfer: {bar} {transfer_pct:5.1f}%")
        print(f"     {format_size(done_size)} / {format_size(info.total_size)} "
              f"| Files: {done_files}/{info.file_count} uploaded")

    # Column headers
    show_drive = drive_status is not None
    if show_drive:
        print(f"\n {'#':<5} {'Name':<38} {'Size':>10}   {'Drive Status'}")
        print(f" {'───':<5} {'─' * 38} {'─' * 10}   {'─' * 14}")
    else:
        print(f"\n {'#':<5} {'Name':<38} {'Size':>10}")
        print(f" {'───':<5} {'─' * 38} {'─' * 10}")

    # File groups
    for group in info.groups:
        # Group header line
        if group.is_directory:
            icon = "📁"
            name_display = f"{group.name}/"
        else:
            icon = "📄"
            name_display = group.name

        size_str = format_size(group.total_size)
        idx_str = f"[{group.group_index}]"

        # Get group-level drive status (aggregate)
        group_drive_str = ""
        if show_drive:
            group_drive_str = _get_group_drive_status(group, drive_status)

        if show_drive:
            print(f" {idx_str:<5} {icon} {name_display:<36} {size_str:>10}   {group_drive_str}")
        else:
            print(f" {idx_str:<5} {icon} {name_display:<36} {size_str:>10}")

        # Show nested files (only for directories with multiple files)
        if group.is_directory and len(group.files) > 0:
            for i, f in enumerate(group.files):
                is_last = (i == len(group.files) - 1)
                connector = "└──" if is_last else "├──"
                file_size_str = format_size(f.size)

                file_drive_str = ""
                if show_drive and f.path in drive_status:
                    file_drive_str = drive_status[f.path]

                if show_drive:
                    print(f"       {connector} {f.name:<34} {file_size_str:>10}   {file_drive_str}")
                else:
                    print(f"       {connector} {f.name:<34} {file_size_str:>10}")

    # Footer
    print(f" {'─' * (width - 1)}")
    print()


def _get_group_drive_status(group, drive_status):
    """
    Aggregate drive status for a group of files.

    :param group: TorrentGroup object
    :param drive_status: dict mapping file paths to status strings
    :return: aggregated status string
    """
    if not drive_status:
        return ""

    statuses = []
    for f in group.files:
        if f.path in drive_status:
            statuses.append(drive_status[f.path])

    if not statuses:
        return "⏳ Pending"

    done_count = sum(1 for s in statuses if "Done" in s)
    if done_count == len(group.files):
        return "✅ Done"
    elif done_count > 0:
        pct = int(done_count / len(group.files) * 100)
        return f"⬇ {pct}%"
    else:
        # Check if any are partially downloaded
        partial = [s for s in statuses if "%" in s and "Done" not in s]
        if partial:
            return partial[0]  # show first partial status
        return "⏳ Pending"


def resolve_selected_indices(info, selection_str):
    """
    Parse user selection string and return the corresponding libtorrent file indices.

    :param info: TorrentInfo object
    :param selection_str: "all" or comma-separated group indices like "0,2,5"
    :return: list of libtorrent file indices to download, or None for all files
    """
    if selection_str.strip().lower() == "all":
        return None  # None means download everything

    try:
        selected_groups = [int(x.strip()) for x in selection_str.split(",")]
    except ValueError:
        logger.error(f"Invalid selection: {selection_str}. Use 'all' or comma-separated numbers.")
        return None

    # Validate indices
    max_idx = max(g.group_index for g in info.groups) if info.groups else -1
    for idx in selected_groups:
        if idx < 0 or idx > max_idx:
            logger.warning(f"Index {idx} is out of range (0-{max_idx}), ignoring.")

    # Collect libtorrent file indices from selected groups
    file_indices = []
    for group in info.groups:
        if group.group_index in selected_groups:
            file_indices.extend(group.file_indices)

    return file_indices


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python torrent_inspector.py <torrent_file/magnet_link>")
        sys.exit(1)

    source = sys.argv[1]
    info = inspect_torrent(source)
    if info:
        display_torrent_info(info)
    else:
        print("Failed to fetch torrent metadata.")
        sys.exit(1)
