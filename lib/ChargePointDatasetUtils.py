"""Shared configuration, logging and atomic CSV storage."""
import configparser
import logging
import os
from pathlib import Path
import tempfile
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
ID_TYPES = {name: 'string' for name in ('session_id', 'user_id', 'credential_id', 'station_id', 'org_id', 'port_no')}

def load_config(path=None):
    """Read private configuration; never fall back to a repository config.ini."""
    selected = path or os.environ.get('SFU_EVP_CONFIG') or Path.home() / '.config' / 'sfu-evp' / 'config.ini'
    path = Path(selected).expanduser().resolve()
    config = configparser.ConfigParser(interpolation=None)
    if not config.read(path):
        raise ValueError(
            f'Configuration file not found: {path}. Copy config.example.ini to '
            '~/.config/sfu-evp/config.ini, restrict its permissions to 600, '
            'and fill in your credentials. See README.md for setup.'
        )
    # Moving credentials must not relocate dataset outputs into the private directory.
    root = Path(config.get('Paths', 'root', fallback=str(ROOT))).expanduser()
    if not root.is_absolute():
        root = path.parent / root
    return config, root.resolve()

def read_csv(path):
    return pd.read_csv(path, dtype=ID_TYPES, keep_default_na=False) if Path(path).exists() else pd.DataFrame()

def save_csv(frame, path, previous=None):
    """Replace atomically, and leave unchanged files untouched."""
    path = Path(path)
    if previous is not None and path.exists():
        try:
            pd.testing.assert_frame_equal(frame.reset_index(drop=True).fillna(''), previous.reset_index(drop=True).fillna(''), check_dtype=False)
            return False
        except (AssertionError, TypeError):
            pass
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=f'.{path.name}.', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as stream:
            frame.to_csv(stream, index=False)
        import filecmp
        if path.exists() and filecmp.cmp(name, path, shallow=False):
            return False
        os.replace(name, path)
        return True
    finally:
        if os.path.exists(name):
            os.unlink(name)

def get_logger(name, cwd, log_path):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        path = Path(cwd) / log_path
        path.parent.mkdir(parents=True, exist_ok=True)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        for handler in (logging.FileHandler(path), logging.StreamHandler()):
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    return logger
