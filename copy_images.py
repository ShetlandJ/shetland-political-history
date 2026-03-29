#!/usr/bin/env python3
"""
Copy person images from MediaWiki images directory to the Astro site,
using corrected image_ref and headshot_ref values from the database.
"""

import sqlite3
import os
import shutil
import hashlib
import subprocess

SQLITE_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'
MW_IMAGES = '/Users/james/projects/shetland_history/images'
OUTPUT_DIR = '/Users/james/projects/shetland_history/new-site/site/public/images/people'

def find_mw_image(filename):
    """Find a file in MediaWiki's hashed directory structure."""
    # MW stores files as images/a/ab/Filename.ext
    # where a = first char of MD5, ab = first 2 chars of MD5
    # MW uses underscores in the hash, and first letter uppercase
    mw_name = filename.replace(' ', '_')
    mw_name = mw_name[0].upper() + mw_name[1:] if mw_name else mw_name

    md5 = hashlib.md5(mw_name.encode('utf-8')).hexdigest()
    path1 = os.path.join(MW_IMAGES, md5[0], md5[:2], mw_name)
    if os.path.exists(path1):
        return path1

    # Try original filename with spaces
    md5_sp = hashlib.md5(filename.encode('utf-8')).hexdigest()
    path2 = os.path.join(MW_IMAGES, md5_sp[0], md5_sp[:2], filename)
    if os.path.exists(path2):
        return path2

    # Brute force search (skip thumb/ directories)
    target = filename.lower().replace(' ', '_')
    for root, dirs, files in os.walk(MW_IMAGES):
        if '/thumb' in root:
            continue
        for f in files:
            if f.lower().replace(' ', '_') == target:
                return os.path.join(root, f)

    return None

def main():
    db = sqlite3.connect(SQLITE_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Clear existing images
    for f in os.listdir(OUTPUT_DIR):
        os.remove(os.path.join(OUTPUT_DIR, f))
    print("Cleared existing images")

    # Copy main images
    c.execute("SELECT slug, image_ref FROM people WHERE image_ref IS NOT NULL AND image_ref != ''")
    main_count = 0
    main_missing = []
    for row in c.fetchall():
        slug = row['slug']
        image_ref = row['image_ref']
        src = find_mw_image(image_ref)
        if src:
            ext = os.path.splitext(image_ref)[1].lower()
            dst = os.path.join(OUTPUT_DIR, slug + ext)
            shutil.copy2(src, dst)
            main_count += 1
        else:
            main_missing.append((slug, image_ref))

    print(f"Copied {main_count} main images")
    if main_missing:
        print(f"  Missing {len(main_missing)} main images:")
        for slug, ref in main_missing[:10]:
            print(f"    {slug}: {ref}")

    # Copy headshot images
    c.execute("SELECT slug, headshot_ref FROM people WHERE headshot_ref IS NOT NULL AND headshot_ref != ''")
    hs_count = 0
    hs_missing = []
    for row in c.fetchall():
        slug = row['slug']
        headshot_ref = row['headshot_ref']
        src = find_mw_image(headshot_ref)
        if src:
            ext = os.path.splitext(headshot_ref)[1].lower()
            dst = os.path.join(OUTPUT_DIR, slug + '-headshot' + ext)
            shutil.copy2(src, dst)
            hs_count += 1
        else:
            hs_missing.append((slug, headshot_ref))

    print(f"Copied {hs_count} headshot images")
    if hs_missing:
        print(f"  Missing {len(hs_missing)} headshot images:")
        for slug, ref in hs_missing[:10]:
            print(f"    {slug}: {ref}")

    # Optimize: convert all images to JPEG, cap at 600px wide, quality 82
    print("\nOptimizing images...")
    before_total = 0
    after_total = 0
    for f in os.listdir(OUTPUT_DIR):
        fpath = os.path.join(OUTPUT_DIR, f)
        before_size = os.path.getsize(fpath)
        before_total += before_size
        base = os.path.splitext(f)[0]
        out_path = os.path.join(OUTPUT_DIR, base + '.jpg')
        subprocess.run([
            'magick', fpath, '-resize', '600x>', '-quality', '82', '-strip', out_path
        ], check=True)
        after_size = os.path.getsize(out_path)
        after_total += after_size
        # Remove original if it was a different format
        if fpath != out_path and os.path.exists(fpath):
            os.remove(fpath)
    print(f"  Before: {before_total // 1024}KB, After: {after_total // 1024}KB, Saved: {(before_total - after_total) // 1024}KB ({(before_total - after_total) * 100 // before_total}%)")

    # Total
    total = len(os.listdir(OUTPUT_DIR))
    print(f"\nTotal images in output: {total}")

    db.close()

if __name__ == '__main__':
    main()
