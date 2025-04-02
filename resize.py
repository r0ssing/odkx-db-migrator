#!/usr/bin/env python3
import os
import sys
from typing import Dict, List, Tuple
import argparse
import shutil
from collections import defaultdict

# Try to import PIL, but don't fail if it's not available
PIL_AVAILABLE = False
try:
    from PIL import Image
    PIL_AVAILABLE = True
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

def get_sizes():
    """Print a summary of the sizes of each folder under data/attachments."""
    attachments_dir = os.path.join('data', 'attachments')
    
    if not os.path.exists(attachments_dir):
        print(f"Error: Attachments directory {attachments_dir} does not exist")
        sys.exit(1)
    
    # Get a list of all tables with attachments
    tables = [d for d in os.listdir(attachments_dir) if os.path.isdir(os.path.join(attachments_dir, d))]
    
    if not tables:
        print(f"No attachment directories found in {attachments_dir}")
        return
    
    print(f"\nAttachment Size Summary:")
    print(f"{'Table':<20} {'Files':<10} {'Total Size':<15} {'Avg Size':<15}")
    print("-" * 60)
    
    grand_total_size = 0
    grand_total_files = 0
    
    for table in tables:
        table_dir = os.path.join(attachments_dir, table)
        table_size = 0
        file_count = 0
        
        # Walk through all subdirectories
        for root, _, files in os.walk(table_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_size = get_file_size(file_path)
                table_size += file_size
                file_count += 1
        
        # Calculate average file size
        avg_size = table_size / file_count if file_count > 0 else 0
        
        # Print table summary
        print(f"{table:<20} {file_count:<10} {format_size(table_size):<15} {format_size(avg_size):<15}")
        
        grand_total_size += table_size
        grand_total_files += file_count
    
    print("-" * 60)
    grand_avg_size = grand_total_size / grand_total_files if grand_total_files > 0 else 0
    print(f"{'TOTAL':<20} {grand_total_files:<10} {format_size(grand_total_size):<15} {format_size(grand_avg_size):<15}")
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
    
    for table in tables:
        table_dir = os.path.join(attachments_dir, table)
        print(f"\nProcessing table: {table}")
        
        # Backup table directory if needed
        if backup:
            backup_table_dir = os.path.join('data', 'attachments_backup', table)
            if not os.path.exists(backup_table_dir):
                shutil.copytree(table_dir, backup_table_dir)
                print(f"Backed up {table} to {backup_table_dir}")
        
        # Walk through all subdirectories
        for root, _, files in os.walk(table_dir):
            for file in files:
                file_path = os.path.join(root, file)
                
                # Check if it's an image file
                _, ext = os.path.splitext(file)
                if ext.lower() not in ['.jpg', '.jpeg', '.png']:
                    skipped_files += 1
                    continue
                
                total_files += 1
                file_original_size = get_file_size(file_path)
                original_size += file_original_size
                
                try:
                    # Open and resize the image
                    with Image.open(file_path) as img:
                        # Get EXIF data
                        exif = None
                        if 'exif' in img.info:
                            exif = img.info['exif']
                        
                        # Check for orientation in EXIF data and rotate if needed
                        try:
                            # Only for JPEG images
                            if ext.lower() in ['.jpg', '.jpeg'] and exif:
                                from PIL import ExifTags
                                for orientation in ExifTags.TAGS.keys():
                                    if ExifTags.TAGS[orientation] == 'Orientation':
                                        break
                                
                                exif_data = img._getexif()
                                if exif_data and orientation in exif_data:
                                    orientation_value = exif_data[orientation]
                                    
                                    # Apply rotation based on EXIF orientation
                                    if orientation_value == 2:
                                        img = img.transpose(Image.FLIP_LEFT_RIGHT)
                                    elif orientation_value == 3:
                                        img = img.transpose(Image.ROTATE_180)
                                    elif orientation_value == 4:
                                        img = img.transpose(Image.FLIP_TOP_BOTTOM)
                                    elif orientation_value == 5:
                                        img = img.transpose(Image.FLIP_LEFT_RIGHT).transpose(Image.ROTATE_90)
                                    elif orientation_value == 6:
                                        img = img.transpose(Image.ROTATE_270)
                                    elif orientation_value == 7:
                                        img = img.transpose(Image.FLIP_LEFT_RIGHT).transpose(Image.ROTATE_270)
                                    elif orientation_value == 8:
                                        img = img.transpose(Image.ROTATE_90)
                        except Exception as e:
                            print(f"  Warning: Could not process EXIF orientation for {file}: {e}")
                        
                        # Calculate new dimensions while preserving aspect ratio
                        width, height = img.size
                        if width > max_dimension or height > max_dimension:
                            if width > height:
                                new_height = int(height * (max_dimension / width))
                                new_width = max_dimension
                            else:
                                new_width = int(width * (max_dimension / height))
                                new_height = max_dimension
                            
                            # Resize the image
                            resized_img = img.resize((new_width, new_height), Image.LANCZOS)
                            
                            # Save with specified quality and preserve EXIF data for JPEGs
                            if ext.lower() in ['.jpg', '.jpeg'] and exif:
                                resized_img.save(file_path, quality=quality, optimize=True, exif=exif)
                            else:
                                resized_img.save(file_path, quality=quality, optimize=True)
                            
                            # Update statistics
                            file_new_size = get_file_size(file_path)
                            new_size += file_new_size
                            
                            reduction = (1 - (file_new_size / file_original_size)) * 100
                            print(f"  Resized {file}: {format_size(file_original_size)} → {format_size(file_new_size)} ({reduction:.1f}% reduction)")
                            
                            processed_files += 1
                        else:
                            # Image is already smaller than max dimension
                            new_size += file_original_size
                            skipped_files += 1
                
                except Exception as e:
                    print(f"  Error processing {file}: {e}")
                    error_files += 1
                    new_size += file_original_size  # Add original size since file wasn't changed
    
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
    
    args = parser.parse_args()
    
    if args.command == 'get_sizes':
        get_sizes()
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
