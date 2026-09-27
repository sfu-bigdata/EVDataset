"""Download the files published at the current IEEE DataPort destination."""
import os
from pathlib import Path
import tempfile

BUCKET = 'ieee-dataport'
PREFIX = 'open/27422/11280/'


def list_dataset_files(client):
    """Return only direct CSV and log objects in this dataset's S3 prefix."""
    files = []
    pages = client.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=PREFIX)
    for page in pages:
        for item in page.get('Contents', []):
            key = item['Key']
            if not key.startswith(PREFIX):
                continue
            name = key[len(PREFIX):]
            if not name or '/' in name or '\\' in name or name.startswith('.'):
                continue
            suffix = Path(name).suffix.lower()
            if suffix in ('.csv', '.log'):
                files.append((key, 'data' if suffix == '.csv' else 'log', name))
    return sorted(set(files))


def confirm_overwrite(name):
    while True:
        try:
            answer = input(f'overwrite {name} (y/n)? ').strip().lower()
        except EOFError:
            print('No input available; keeping the local file.')
            return False
        if answer == 'y':
            return True
        if answer == 'n':
            return False
        print('Please enter y or n.')


def download_files(client, root, *, dry_run=False):
    files = list_dataset_files(client)
    if not files:
        raise ValueError('No CSV or log files found in the published dataset')
    downloaded = skipped = failed = 0
    for key, folder, name in files:
        target = Path(root) / folder / name
        if dry_run:
            action = 'ask before overwriting' if target.exists() or target.is_symlink() else 'download'
            print(f's3://{BUCKET}/{key} -> {target} ({action})')
            continue
        temporary = None
        try:
            if target.is_symlink() or target.parent.is_symlink():
                raise ValueError('Refusing to download through a symbolic link')
            existed = target.exists()
            if existed:
                if not target.is_file():
                    raise ValueError('Destination exists and is not a regular file')
                if not confirm_overwrite(name):
                    print(f'Skipped {name}')
                    skipped += 1
                    continue
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(prefix=f'.{name}.', suffix='.tmp', dir=target.parent)
            os.close(fd)
            print(f'Downloading {name}...')
            client.download_file(BUCKET, key, temporary)
            # Do not silently overwrite a file that appeared during the transfer.
            if not existed and (target.exists() or target.is_symlink()):
                if not confirm_overwrite(name):
                    print(f'Skipped {name}')
                    skipped += 1
                    continue
            os.replace(temporary, target)
            downloaded += 1
            print(f'Saved {target}')
        except Exception as exc:
            failed += 1
            print(f'Failed to download {name}: {exc}')
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)
    if not dry_run:
        print(f'Downloaded: {downloaded}; skipped: {skipped}; failed: {failed}')
    return 1 if failed else 0
