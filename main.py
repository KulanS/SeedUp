#!/usr/bin/env python3
"""
SeedUp - Smart Torrent Management Tool
A Python-based tool that combines torrent downloading with Google Drive uploading capabilities.

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

Main entry point for torrent downloader with Google Drive upload (Colab-optimized).
Combines torrent downloading and cloud storage capabilities.
"""

import sys
import argparse
import os
from pathlib import Path

from torrent_downloader import download_torrent, get_download_status, clear_session
from config import ConfigManager, TORRENT_DOWNLOAD_PATH, get_logger

logger = get_logger(__name__)


def get_uploader():
    """Import and return uploader module."""
    try:
        from gdrive_uploader import upload_to_google_drive
        return upload_to_google_drive
    except ImportError as e:
        logger.error(f"Failed to import uploader: {str(e)}")
        print("\n" + "="*60)
        print("ERROR: Failed to import Google Drive uploader")
        print("="*60)
        print("Please ensure all required packages are installed:")
        print("  pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client")
        print("="*60)
        raise


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Download torrents and upload to Google Drive (Colab-optimized)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download torrent only
  python main.py download -t movie.torrent
  python main.py download -t "magnet:?xt=urn:btih:..."
  
  # Download and upload to Google Drive (Colab only)
  python main.py download -t movie.torrent --upload -f FOLDER_ID
  
  # Upload existing files to Google Drive (Colab only)
  python main.py upload -p /path/to/folder -f FOLDER_ID
  
  # Upload without skipping existing files
  python main.py upload -p /path -f FOLDER_ID --no-skip
  
  # Check for paused downloads
  python main.py status
  
  # Clear download session
  python main.py clear
  
Note: Upload features automatically handle authentication in Google Colab.
Just run your commands directly - no manual setup required!
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Download command
    download_parser = subparsers.add_parser('download', help='Download a torrent')
    download_parser.add_argument(
        '-t', '--torrent',
        type=str,
        required=True,
        help='Torrent file path or magnet link'
    )
    download_parser.add_argument(
        '-d', '--destination',
        type=str,
        default=TORRENT_DOWNLOAD_PATH,
        help=f'Download destination (default: {TORRENT_DOWNLOAD_PATH})'
    )
    download_parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh download (ignore previous session)'
    )
    download_parser.add_argument(
        '--upload',
        action='store_true',
        help='Upload to Google Drive after download (Colab only)'
    )
    download_parser.add_argument(
        '-f', '--folder-id',
        type=str,
        help='Google Drive folder ID (optional, defaults to SeedUp Downloads folder in Drive root)'
    )
    download_parser.add_argument(
        '--no-skip',
        action='store_true',
        help='Force re-upload even if files exist in Drive'
    )
    download_parser.add_argument(
        '--select',
        type=str,
        default='all',
        help='File indices to download: "all" or comma-separated indices from preview (e.g. "0,2,5")'
    )
    
    # Upload command
    upload_parser = subparsers.add_parser('upload', help='Upload files to Google Drive (Colab only)')
    upload_parser.add_argument(
        '-p', '--path',
        type=str,
        required=True,
        help='Local path to file or folder to upload'
    )
    upload_parser.add_argument(
        '-f', '--folder-id',
        type=str,
        help='Google Drive destination folder ID (optional, defaults to SeedUp Downloads folder in Drive root)'
    )
    upload_parser.add_argument(
        '--no-skip',
        action='store_true',
        help='Force re-upload even if files exist in Drive'
    )
    
    # Status command
    subparsers.add_parser('status', help='Check download status')
    
    # Clear command
    subparsers.add_parser('clear', help='Clear download session')
    
    # Preview command
    preview_parser = subparsers.add_parser('preview', help='Preview torrent contents before downloading')
    preview_parser.add_argument(
        '-t', '--torrent',
        type=str,
        required=True,
        help='Torrent file path or magnet link'
    )
    preview_parser.add_argument(
        '--check-drive',
        action='store_true',
        help='Check Google Drive upload status for each file'
    )
    
    return parser.parse_args()


def handle_download(args):
    """Handle torrent download command."""
    
    # Fetch metadata once — used for selection and passed to pipeline
    from torrent_inspector import inspect_torrent, resolve_selected_indices
    info = inspect_torrent(args.torrent)
    if info is None:
        logger.error("Failed to fetch torrent metadata")
        return 1

    # Resolve file selection
    selected_indices = None
    if args.select and args.select.strip().lower() != 'all':
        selected_indices = resolve_selected_indices(info, args.select)
        if selected_indices is not None and len(selected_indices) == 0:
            logger.error("No valid files selected. Use 'all' or valid indices.")
            return 1
    
    # Use pipeline when --upload is set (incremental download + upload)
    if args.upload:
        from pipeline import run_pipeline
        
        result = run_pipeline(
            source=args.torrent,
            download_path=args.destination,
            auto_resume=not args.no_resume,
            selected_indices=selected_indices,
            folder_id=args.folder_id,
            skip_existing=not args.no_skip,
            torrent_info=info,  # pass pre-fetched metadata
        )
        
        return 0 if result.success else 1
    
    # Legacy: download-only mode (no upload)
    print("="*60)
    print("TORRENT DOWNLOADER")
    print("="*60)
    
    # Use cached .torrent file if available (avoids re-fetching magnet metadata)
    dl_source = args.torrent
    if info.cached_torrent_path and os.path.exists(info.cached_torrent_path):
        dl_source = info.cached_torrent_path

    logger.info(f"Starting download: {args.torrent}")
    downloaded_path = download_torrent(
        dl_source,
        download_path=args.destination,
        auto_resume=not args.no_resume,
        selected_indices=selected_indices,
    )
    
    if not downloaded_path:
        logger.error("Download failed or was cancelled")
        return 1
    
    logger.info(f"Download completed: {downloaded_path}")
    return 0


def handle_upload(args):
    """Handle Google Drive upload command."""
    print("="*60)
    print("GOOGLE DRIVE UPLOADER")
    print("="*60)
    
    # Validate path exists
    if not os.path.exists(args.path):
        logger.error(f"Path does not exist: {args.path}")
        return 1
    
    # Upload to Google Drive
    try:
        # Load uploader
        upload_to_google_drive = get_uploader()
        
        results = upload_to_google_drive(
            args.path,
            args.folder_id,
            skip_existing=not args.no_skip
        )
        
        if results['failed']:
            logger.warning(f"Some files failed to upload ({len(results['failed'])} items)")
            return 1
        
        logger.info("Upload completed successfully!")
        return 0
    
    except RuntimeError as e:
        # Catch environment/initialization errors with formatted message
        error_str = str(e)
        if error_str.startswith('\n'):
            # Already formatted, just print it
            print(error_str)
        else:
            # Wrap in formatting
            print("\n" + "="*60)
            print("UPLOAD ERROR")
            print("="*60)
            print(error_str)
            print("="*60)
        return 1
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        return 1


def handle_status(args):
    """Handle status check command."""
    if get_download_status():
        print("✓ Found paused download session")
        print("  Run 'python main.py download -t <torrent>' to resume")
        return 0
    else:
        print("✗ No paused download session found")
        return 0


def handle_clear(args):
    """Handle clear session command."""
    if clear_session():
        print("✓ Download session cleared")
        return 0
    else:
        print("✗ Failed to clear session")
        return 1


def handle_preview(args):
    """Handle torrent preview command."""
    from torrent_inspector import inspect_torrent, display_torrent_info
    
    info = inspect_torrent(args.torrent)
    if info is None:
        logger.error("Failed to fetch torrent metadata")
        return 1
    
    drive_status = None
    drive_space = None
    
    if args.check_drive:
        try:
            from gdrive_uploader import check_drive_status, get_drive_space_info
            drive_status = check_drive_status(
                info.all_files,
                download_path=TORRENT_DOWNLOAD_PATH
            )
            drive_space = get_drive_space_info()
        except Exception as e:
            logger.warning(f"Could not check Drive status: {e}")
    
    display_torrent_info(info, drive_status=drive_status, drive_space=drive_space)
    
    print("💡 To download, run:")
    print(f"   python main.py download -t \"{args.torrent}\" --upload --select all")
    print(f"   python main.py download -t \"{args.torrent}\" --upload --select 0,2,5")
    
    return 0


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Show help if no command specified
    if not args.command:
        print("Error: No command specified\n")
        parse_arguments().print_help()
        return 1
    
    try:
        # Route to appropriate handler
        if args.command == 'download':
            return handle_download(args)
        elif args.command == 'upload':
            return handle_upload(args)
        elif args.command == 'status':
            return handle_status(args)
        elif args.command == 'clear':
            return handle_clear(args)
        elif args.command == 'preview':
            return handle_preview(args)
        else:
            logger.error(f"Unknown command: {args.command}")
            return 1
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        if args.command == 'download':
            print("Download progress has been saved. Resume with the same command.")
        return 130
    except Exception as e:
        logger.error(f"Operation failed: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())