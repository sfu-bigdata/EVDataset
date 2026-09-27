#!/usr/bin/env python3
"""Download published CSVs to data/ and logs to log/, prompting before overwrites."""
import argparse
import sys
from lib.ChargePointDatasetUtils import load_config
from lib.public_dataset import download_files


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', help='Private config path (or SFU_EVP_CONFIG; default: ~/.config/sfu-evp/config.ini)')
    parser.add_argument('--dry-run', action='store_true', help='List remote files and local destinations without downloading')
    args = parser.parse_args(argv)
    try:
        config, root = load_config(args.config)
        key = config.get('DataPort', 'api_key', fallback='')
        secret = config.get('DataPort', 'secret', fallback='')
        if not key or not secret:
            raise ValueError('Set [DataPort] api_key and secret in your private configuration; IEEE DataPort requires authenticated listing')
        import boto3
        from botocore.config import Config
        client = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret,
                              config=Config(connect_timeout=15, read_timeout=120,
                                            retries={'mode': 'standard', 'max_attempts': 3}))
        try:
            return download_files(client, root, dry_run=args.dry_run)
        finally:
            client.close()
    except KeyboardInterrupt:
        print('Download interrupted.', file=sys.stderr)
        return 130
    except Exception as exc:
        print(f'Download failed: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
