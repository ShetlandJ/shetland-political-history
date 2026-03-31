"""Auto-generate headshot crops from main photos for people who lack them."""
import os
from PIL import Image

IMAGES_DIR = os.path.join(os.path.dirname(__file__), '..', 'site', 'public', 'images', 'people')
EXTS = ['.jpg', '.jpeg', '.png', '.gif']
OUTPUT_SIZE = 150  # px square


def find_main_photo(slug):
    for ext in EXTS:
        p = os.path.join(IMAGES_DIR, slug + ext)
        if os.path.exists(p):
            return p
    return None


def has_headshot(slug):
    for ext in EXTS:
        p = os.path.join(IMAGES_DIR, slug + '-headshot' + ext)
        if os.path.exists(p):
            return True
    return False


def auto_crop_headshot(src_path, dst_path):
    """Crop a square from the top-center of the image, biased upward for faces."""
    img = Image.open(src_path)
    w, h = img.size

    # Take a square from the top portion
    sq = min(w, h)

    if w >= h:
        # Landscape or square: crop center horizontally, from top
        left = (w - sq) // 2
        box = (left, 0, left + sq, sq)
    else:
        # Portrait: crop full width, top-biased
        # Take top 70% area for the face crop
        crop_h = w  # square side = width
        top = min(int(h * 0.05), h - crop_h)  # slight offset from very top
        top = max(0, top)
        box = (0, top, w, top + crop_h)

    cropped = img.crop(box)
    cropped = cropped.resize((OUTPUT_SIZE, OUTPUT_SIZE), Image.LANCZOS)
    cropped.save(dst_path, 'JPEG', quality=85)


def main():
    # Find all main photos
    all_files = os.listdir(IMAGES_DIR)
    main_slugs = set()
    for f in all_files:
        if '-headshot' in f:
            continue
        name, ext = os.path.splitext(f)
        if ext.lower() in EXTS:
            main_slugs.add(name)

    generated = []
    for slug in sorted(main_slugs):
        if has_headshot(slug):
            continue
        src = find_main_photo(slug)
        if not src:
            continue
        dst = os.path.join(IMAGES_DIR, slug + '-headshot.jpg')
        auto_crop_headshot(src, dst)
        generated.append(slug)
        print(f'Generated: {slug}-headshot.jpg')

    print(f'\nTotal: {len(generated)} headshots generated')


if __name__ == '__main__':
    main()
