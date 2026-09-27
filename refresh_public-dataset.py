#!/usr/bin/env python3
"""Upload the dataset and logs to IEEE DataPort once, in the foreground."""
import argparse
import sys
from lib.ChargePointDatasetUtils import load_config, get_logger

BUCKET = 'ieee-dataport'
PREFIX = 'open/27422/11280/'

def upload_files(client, files, logger):
    failures = 0
    for number, path in enumerate(files, 1):
        try:
            client.upload_file(str(path), BUCKET, PREFIX + path.name)
            logger.info('[%d/%d] Uploaded %s', number, len(files), path.name)
        except Exception as exc:
            failures += 1
            logger.error('Failed to upload %s: %s', path.name, exc)
    return 1 if failures else 0

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=None, help='Private config path (or SFU_EVP_CONFIG; default: ~/.config/sfu-evp/config.ini)')
    parser.add_argument('--dry-run', action='store_true', help='List files without connecting to DataPort')
    args = parser.parse_args(argv)
    try:
        config, root = load_config(args.config)
        files = sorted(p for folder in ('data', 'log') for p in (root / folder).glob('*')
                       if p.is_file() and not p.name.startswith('.') and not p.name.endswith('.tmp'))
        if not files:
            raise ValueError('No data or log files found')
        if len({p.name for p in files}) != len(files):
            raise ValueError('Duplicate filenames would overwrite the same DataPort object')
        if args.dry_run:
            for path in files:
                print(f'{path} -> s3://{BUCKET}/{PREFIX}{path.name}')
            return 0
        import boto3
        from botocore.config import Config
        logger = get_logger('Upload', root, 'log/upload.log')
        client = boto3.client('s3', aws_access_key_id=config.get('DataPort', 'api_key'),
                          aws_secret_access_key=config.get('DataPort', 'secret'),
                          config=Config(connect_timeout=15, read_timeout=120,
                                        retries={'mode': 'standard', 'max_attempts': 3}))
        try:
            return upload_files(client, files, logger)
        finally:
            client.close()
    except KeyboardInterrupt:
        print('Upload interrupted.', file=sys.stderr)
        return 130
    except Exception as exc:
        print(f'Upload failed: {exc}', file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())
