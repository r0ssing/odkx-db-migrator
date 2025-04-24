#!/usr/bin/env python3
import os
import sys
from typing import Dict, List, Tuple
import argparse
import shutil
from collections import defaultdict, Counter
from tqdm import tqdm

# Try to import progress_bar_iter, but don't fail if it's not available
try:
    from src.utils import progress_bar_iter
except ImportError:
    # Define a simple fallback if the import fails
    def progress_bar_iter(iterable, **kwargs):
        return tqdm(iterable, **kwargs)

# Try to import PIL, but don't fail if it's not available
PIL_AVAILABLE = False
PIEXIF_AVAILABLE = False
try:
    from PIL import Image, ExifTags
    PIL_AVAILABLE = True
    try:
        import piexif
        PIEXIF_AVAILABLE = True
    except ImportError:
        pass
except ImportError:
    pass

def get_file_size(file_path: str) -> int:
    """Get the size of a file in bytes."""
    return os.path.getsize(file_path)

def format_size(size_bytes: int) -> str:
    """Format a size in bytes to a human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def get_sizes(test_all=False):
    """Print a summary of the sizes of each folder under data/attachments.
    
    Args:
        test_all: If True, check dimensions of all images (slower but more accurate)
                  If False (default), only check the first 10 images per table
    """
    attachments_dir = os.path.join('data', 'attachments')
    
    if not os.path.exists(attachments_dir):
        print(f"Error: Attachments directory {attachments_dir} does not exist")
        sys.exit(1)
    
    # Get a list of all tables with attachments
    tables = [d for d in os.listdir(attachments_dir) if os.path.isdir(os.path.join(attachments_dir, d))]
    
    if not tables:
        print(f"No attachment directories found in {attachments_dir}")
        return
    
    # First pass: collect data and determine column widths
    table_data = []
    max_table_name_len = len("Table")
    max_files_len = len("Files")
    max_total_size_len = len("Total Size")
    max_avg_size_len = len("Avg Size")
    max_dimensions_len = len("Dimensions")
    
    grand_total_size = 0
    grand_total_files = 0
    all_dimensions = Counter()
    
    for table in tables:
        table_dir = os.path.join(attachments_dir, table)
        table_size = 0
        file_count = 0
        dimensions_counter = Counter()
        
        # Track how many images we've processed for this table (for quick mode)
        images_processed = 0
        
        # Walk through all subdirectories
        for root, _, files in os.walk(table_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_size = get_file_size(file_path)
                table_size += file_size
                file_count += 1
                
                # Get image dimensions if it's an image file
                _, ext = os.path.splitext(file)
                if PIL_AVAILABLE and ext.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff']:
                    # By default, only process the first 10 images per table
                    # If test_all is True, process all images
                    if not test_all and images_processed >= 10:
                        continue
                        
                    try:
                        with Image.open(file_path) as img:
                            width, height = img.size
                            # Format as greatest_dimension x smallest_dimension
                            if width >= height:
                                dimensions = f"{width}x{height}"
                            else:
                                dimensions = f"{height}x{width}"
                            dimensions_counter[dimensions] += 1
                            all_dimensions[dimensions] += 1
                            images_processed += 1
                    except Exception:
                        # Skip files that can't be opened as images
                        pass
        
        # Calculate average file size
        avg_size = table_size / file_count if file_count > 0 else 0
        
        # Get most common dimensions
        most_common_dimensions = "N/A"
        if dimensions_counter:
            most_common_dimensions = dimensions_counter.most_common(1)[0][0]
        
        # Store data for later display
        formatted_total_size = format_size(table_size)
        formatted_avg_size = format_size(avg_size)
        table_data.append({
            'table': table,
            'file_count': file_count,
            'total_size': formatted_total_size,
            'avg_size': formatted_avg_size,
            'dimensions': most_common_dimensions
        })
        
        # Update maximum column widths
        max_table_name_len = max(max_table_name_len, len(table))
        max_files_len = max(max_files_len, len(str(file_count)))
        max_total_size_len = max(max_total_size_len, len(formatted_total_size))
        max_avg_size_len = max(max_avg_size_len, len(formatted_avg_size))
        max_dimensions_len = max(max_dimensions_len, len(most_common_dimensions))
        
        grand_total_size += table_size
        grand_total_files += file_count
    
    # Add spacing between columns
    col_spacing = 4
    total_width = (max_table_name_len + max_files_len + max_total_size_len + 
                  max_avg_size_len + max_dimensions_len + (col_spacing * 4))
    
    # Calculate grand average size
    grand_avg_size = grand_total_size / grand_total_files if grand_total_files > 0 else 0
    
    # Format the grand totals
    formatted_grand_total_size = format_size(grand_total_size)
    formatted_grand_avg_size = format_size(grand_avg_size)
    
    # Get most common dimensions overall
    most_common_overall = "N/A"
    if all_dimensions:
        most_common_overall = all_dimensions.most_common(1)[0][0]
    
    # Update max widths for totals row
    max_table_name_len = max(max_table_name_len, len("TOTAL"))
    max_files_len = max(max_files_len, len(str(grand_total_files)))
    max_total_size_len = max(max_total_size_len, len(formatted_grand_total_size))
    max_avg_size_len = max(max_avg_size_len, len(formatted_grand_avg_size))
    max_dimensions_len = max(max_dimensions_len, len(most_common_overall))
    
    # Now print the header with adjusted column widths
    mode_text = " (testing all images)" if test_all else ""
    print(f"\nAttachment Size Summary{mode_text}:")
    
    header = (f"{'Table':<{max_table_name_len}}" + " " * col_spacing +
              f"{'Files':<{max_files_len}}" + " " * col_spacing +
              f"{'Total Size':<{max_total_size_len}}" + " " * col_spacing +
              f"{'Avg Size':<{max_avg_size_len}}" + " " * col_spacing +
              f"{'Dimensions':<{max_dimensions_len}}")
    print(header)
    print("-" * total_width)
    
    # Print each row with adjusted column widths
    for row in table_data:
        print(f"{row['table']:<{max_table_name_len}}" + " " * col_spacing +
              f"{row['file_count']:<{max_files_len}}" + " " * col_spacing +
              f"{row['total_size']:<{max_total_size_len}}" + " " * col_spacing +
              f"{row['avg_size']:<{max_avg_size_len}}" + " " * col_spacing +
              f"{row['dimensions']:<{max_dimensions_len}}")
    
    # Print the separator and totals row
    print("-" * total_width)
    print(f"{'TOTAL':<{max_table_name_len}}" + " " * col_spacing +
          f"{grand_total_files:<{max_files_len}}" + " " * col_spacing +
          f"{formatted_grand_total_size:<{max_total_size_len}}" + " " * col_spacing +
          f"{formatted_grand_avg_size:<{max_avg_size_len}}" + " " * col_spacing +
          f"{most_common_overall:<{max_dimensions_len}}")
    print()

def get_detailed_sizes():
    """Print a detailed summary of file types and their sizes under data/attachments."""
    attachments_dir = os.path.join('data', 'attachments')
    
    if not os.path.exists(attachments_dir):
        print(f"Error: Attachments directory {attachments_dir} does not exist")
        sys.exit(1)
    
    # Track sizes by file extension
    extension_sizes = defaultdict(lambda: {'count': 0, 'total_size': 0})
    
    # Walk through all subdirectories
    for root, _, files in os.walk(attachments_dir):
        for file in files:
            file_path = os.path.join(root, file)
            file_size = get_file_size(file_path)
            
            # Get file extension (lowercase)
            _, ext = os.path.splitext(file)
            ext = ext.lower() if ext else 'no_extension'
            
            # Update statistics
            extension_sizes[ext]['count'] += 1
            extension_sizes[ext]['total_size'] += file_size
    
    # Print summary by file type
    print(f"\nFile Type Summary:")
    print(f"{'Extension':<15} {'Files':<10} {'Total Size':<15} {'Avg Size':<15}")
    print("-" * 60)
    
    # Sort by total size (descending)
    sorted_extensions = sorted(
        extension_sizes.items(),
        key=lambda x: x[1]['total_size'],
        reverse=True
    )
    
    grand_total_size = 0
    grand_total_files = 0
    
    for ext, stats in sorted_extensions:
        count = stats['count']
        total_size = stats['total_size']
        avg_size = total_size / count if count > 0 else 0
        
        print(f"{ext:<15} {count:<10} {format_size(total_size):<15} {format_size(avg_size):<15}")
        
        grand_total_size += total_size
        grand_total_files += count
    
    print("-" * 60)
    grand_avg_size = grand_total_size / grand_total_files if grand_total_files > 0 else 0
    print(f"{'TOTAL':<15} {grand_total_files:<10} {format_size(grand_total_size):<15} {format_size(grand_avg_size):<15}")
    print()

def resize_images(max_dimension: int = 1024, quality: int = 85, backup: bool = True, table: str = None):
    """Resize all images in the attachments directory to reduce file size.
    
    Args:
        max_dimension: Maximum width or height in pixels (preserves aspect ratio)
        quality: JPEG quality (0-100, higher is better quality but larger file)
        backup: If True, create a backup of original images before resizing
        table: If provided, only resize images for the specified table
    """
    attachments_dir = os.path.join('data', 'attachments')
    
    if not os.path.exists(attachments_dir):
        print(f"Error: Attachments directory {attachments_dir} does not exist")
        sys.exit(1)
    
    # Create backup directory if needed
    if backup:
        backup_dir = os.path.join('data', 'attachments_backup')
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
            print(f"Created backup directory: {backup_dir}")
    
    # Get tables to process
    if table:
        table_dir = os.path.join(attachments_dir, table)
        if not os.path.exists(table_dir):
            print(f"Error: Table directory {table_dir} does not exist")
            sys.exit(1)
        tables = [table]
    else:
        tables = [d for d in os.listdir(attachments_dir) if os.path.isdir(os.path.join(attachments_dir, d))]
    
    if not tables:
        print(f"No attachment directories found to process")
        return
    
    print(f"\nResizing images with max dimension {max_dimension}px and quality {quality}%")
    
    # Track statistics
    total_files = 0
    processed_files = 0
    skipped_files = 0
    error_files = 0
    original_size = 0
    new_size = 0
    
    # First, collect all image files to process
    image_files = []
    for table in tables:
        table_dir = os.path.join(attachments_dir, table)
        # Backup table directory if needed
        if backup:
            backup_table_dir = os.path.join('data', 'attachments_backup', table)
            if not os.path.exists(backup_table_dir):
                shutil.copytree(table_dir, backup_table_dir)
                print(f"Backed up {table} to {backup_table_dir}")
        for root, _, files in os.walk(table_dir):
            for file in files:
                _, ext = os.path.splitext(file)
                if ext.lower() in ['.jpg', '.jpeg', '.png']:
                    image_files.append((root, file, table))
    total_files = len(image_files)
    if total_files == 0:
        print("No image files found to process.")
        return
    
    with tqdm(image_files, total=total_files, desc="Resizing images", ncols=100,
             bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as bar:
        for root, file, table in bar:
            file_path = os.path.join(root, file)
            file_original_size = get_file_size(file_path)
            original_size += file_original_size
            postfix = {"file": f"{table}/{file}"}
            try:
                with Image.open(file_path) as img:
                    exif = img.info.get('exif') if 'exif' in img.info else None
                    try:
                        _, ext = os.path.splitext(file)
                        if ext.lower() in ['.jpg', '.jpeg'] and exif:
                            from PIL import ExifTags
                            for orientation in ExifTags.TAGS.keys():
                                if ExifTags.TAGS[orientation] == 'Orientation':
                                    break
                            exif_data = img._getexif()
                            if exif_data and orientation in exif_data:
                                orientation_value = exif_data[orientation]
                                postfix["exif"] = f"EXIF {orientation_value}"
                    except Exception as e:
                        postfix["warn"] = f"EXIF warn"
                    width, height = img.size
                    if width > max_dimension or height > max_dimension:
                        if width > height:
                            new_height = int(height * (max_dimension / width))
                            new_width = max_dimension
                        else:
                            new_width = int(width * (max_dimension / height))
                            new_height = max_dimension
                        resized_img = img.resize((new_width, new_height), Image.LANCZOS)
                        if ext.lower() in ['.jpg', '.jpeg']:
                            try:
                                if exif:
                                    resized_img.save(file_path, quality=quality, optimize=True, exif=exif)
                                else:
                                    resized_img.save(file_path, quality=quality, optimize=True)
                            except Exception as e:
                                postfix["warn"] = "Save warn"
                                try:
                                    resized_img.save(file_path, quality=quality, optimize=True)
                                except Exception as e2:
                                    postfix["error"] = "Save fail"
                        else:
                            resized_img.save(file_path, quality=quality, optimize=True)
                        file_new_size = get_file_size(file_path)
                        new_size += file_new_size
                        reduction = (1 - (file_new_size / file_original_size)) * 100
                        postfix["reduction"] = f"{reduction:.1f}%"
                        processed_files += 1
                    else:
                        new_size += file_original_size
                        skipped_files += 1
            except Exception as e:
                postfix["error"] = "Proc fail"
                error_files += 1
                new_size += file_original_size
            bar.set_postfix(postfix, refresh=True)
    # Print summary
    print("\n" + "="*60)
    print("Resize Summary:")
    print(f"Total files scanned: {total_files}")
    print(f"Files resized: {processed_files}")
    print(f"Files skipped (non-image or already small): {skipped_files}")
    print(f"Files with errors: {error_files}")
    if original_size > 0:
        reduction = (1 - (new_size / original_size)) * 100
        print(f"Original size: {format_size(original_size)}")
        print(f"New size: {format_size(new_size)}")
        print(f"Reduction: {reduction:.1f}%")
    print("="*60)

def help():
    """Display all available commands with their descriptions."""
    # Check PIL availability at runtime
    pil_available = False
    try:
        import PIL
        pil_available = True
    except ImportError:
        pass
        
    print("\nAvailable Commands:")
    print("==================================================")
    print(f"{'get_sizes':<25} - Print a summary of the sizes of each folder under data/attachments.")
    print(f"{'get_detailed_sizes':<25} - Print a detailed summary of file types and their sizes under data/attachments.")
    if pil_available:
        print(f"{'resize_images':<25} - Resize all images in the attachments directory to reduce file size.")
    print(f"{'help':<25} - Display all available commands with their descriptions.")
    
    print("\nCommand-line Usage:")
    print("==================================================")
    print("  python resize.py <command> [options]")
    
    if pil_available:
        print("\nOptions for resize_images:")
        print(f"  {'--max-dimension':<20} - Maximum width or height in pixels (default: 1024)")
        print(f"  {'--quality':<20} - JPEG quality (0-100, higher is better quality but larger file) (default: 85)")
        print(f"  {'--no-backup':<20} - Skip creating backup of original images")
        print(f"  {'--table':<20} - Only process images for the specified table")
    
    print("\nSee also:")
    print("  Database and attachment management helper functions:")
    print("    python helpers.py")
    print("    (e.g., pull_database, push_database, get_forms_with_attachments, remove_empty_files, etc.)")

def main():
    if len(sys.argv) < 2 or sys.argv[1] == 'help':
        help()
        return

    # Define available commands based on dependencies
    available_commands = ['get_sizes', 'get_detailed_sizes']
    if PIL_AVAILABLE:
        available_commands.append('resize_images')

    parser = argparse.ArgumentParser(description="Attachment resizing and analysis tools")
    parser.add_argument('command', choices=available_commands, 
                        help='Command to execute')
    
    # Arguments for resize_images
    if PIL_AVAILABLE:
        parser.add_argument('--max-dimension', type=int, default=1024,
                            help='Maximum width or height in pixels (default: 1024)')
        parser.add_argument('--quality', type=int, default=85,
                            help='JPEG quality (0-100, higher is better quality but larger file) (default: 85)')
        parser.add_argument('--no-backup', action='store_true',
                            help='Skip creating backup of original images')
        parser.add_argument('--table', type=str,
                            help='Only process images for the specified table')
    
    # Arguments for get_sizes
    parser.add_argument('--testall', action='store_true',
                        help='Check dimensions of all images (slower but more accurate)')
    
    args = parser.parse_args()
    
    if args.command == 'get_sizes':
        get_sizes(test_all=args.testall if hasattr(args, 'testall') else False)
    elif args.command == 'get_detailed_sizes':
        get_detailed_sizes()
    elif args.command == 'resize_images':
        if PIL_AVAILABLE:
            resize_images(
                max_dimension=args.max_dimension,
                quality=args.quality,
                backup=not args.no_backup,
                table=args.table
            )
        else:
            print("Error: PIL library is not available. Please install it to use the resize_images command.")

if __name__ == '__main__':
    main()
