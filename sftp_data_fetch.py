from dotenv import load_dotenv
load_dotenv() 

import os
import stat
import posixpath
import zipfile
import time
import json
import logging
from logging.handlers import RotatingFileHandler
import paramiko

remote_to_local = {
    "/home/techaff/NiFi Testing/sales_transaction/HOT": r"D:\etl_hyperforge\NiFi Testing\sales_transaction\HOT",
    "/home/techaff/NiFi Testing/sales_transaction/RET": r"D:\etl_hyperforge\NiFi Testing\sales_transaction\RET",
    "/home/techaff/NiFi Testing/sales_transaction/BSP": r"D:\etl_hyperforge\NiFi Testing\sales_transaction\BSP",
    "/home/techaff/NiFi Testing/sales_transaction/ARC": r"D:\etl_hyperforge\NiFi Testing\sales_transaction\ARC",
    "/home/techaff/NiFi Testing/atpco": r"D:\etl_hyperforge\NiFi Testing\atpco",
}

manifest_path = "processed_zips.json"
log_path = "sftp_download.log"

handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[handler, logging.StreamHandler()],
)

def load_manifest():
    if os.path.exists(manifest_path):
        with open(manifest_path, "r") as f:
            return {k: set(v) for k, v in json.load(f).items()}
    return {}

def save_manifest(manifest):
    with open(manifest_path, "w") as f:
        json.dump({k: list(v) for k, v in manifest.items()}, f, indent=4)

def walk_remote(sftp, root):
    for entry in sftp.listdir_attr(root):
        path = posixpath.join(root, entry.filename)
        if stat.S_ISDIR(entry.st_mode):
            yield from walk_remote(sftp, path)
        elif stat.S_ISLNK(entry.st_mode):
            continue
        else:
            yield path

def download_and_process():
    host = os.getenv("SFTP_HOST") or _raise("SFTP_HOST")
    port = int(os.getenv("SFTP_PORT") or _raise("SFTP_PORT"))
    user = os.getenv("SFTP_USER") or _raise("SFTP_USER")
    pwd  = os.getenv("SFTP_PASS") or _raise("SFTP_PASS")

    ssh = paramiko.SSHClient()
    known = os.getenv("SFTP_KNOWN_HOSTS")
    if known and os.path.exists(known):
        ssh.load_host_keys(known)
    else:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(
        hostname=host,
        port=port,
        username=user,
        password=pwd,
        look_for_keys=False,
    )
    ssh.get_transport().set_keepalive(30)

    manifest = load_manifest()

    with ssh.open_sftp() as sftp:
        logging.info("SFTP Connection established.")
        for remote_root, local_root in remote_to_local.items():
            processed = manifest.get(local_root, set())
            for remote_file in walk_remote(sftp, remote_root):
                rel_path = posixpath.relpath(remote_file, remote_root)
                if rel_path in processed:
                    continue

                local_file = os.path.join(local_root, rel_path)
                os.makedirs(os.path.dirname(local_file), exist_ok=True)

                tmp_file = local_file + ".part"
                logging.info(f"[DOWNLOADING] {remote_file}")
                try:
                    sftp.get(remote_file, tmp_file)
                    os.replace(tmp_file, local_file)
                except Exception as e:
                    logging.error(f"[ERROR] download {remote_file}: {e}")
                    if os.path.exists(tmp_file):
                        os.remove(tmp_file)
                    continue

                if local_file.lower().endswith(".zip"):
                    try:
                        with zipfile.ZipFile(local_file) as zf:
                            zf.extractall(os.path.dirname(local_file))
                        os.remove(local_file)
                        logging.info(f"[UNZIPPED] {rel_path}")
                    except Exception as e:
                        logging.error(f"[ERROR] unzip {rel_path}: {e}")
                        continue

                processed.add(rel_path)
            manifest[local_root] = processed

    save_manifest(manifest)
    logging.info("All downloads and extractions complete.")

def _raise(var):
    raise RuntimeError(f"Environment variable {var!r} is missing")

if __name__ == "__main__":
    download_and_process()