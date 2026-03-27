#!/usr/bin/env python3

import json
import os
from pathlib import Path
from paramiko import Transport, SFTPClient
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("SFTP_HOST")
PORT = int(os.getenv("SFTP_PORT", "2222"))
USERNAME = os.getenv("SFTP_USERNAME")
PASSWORD = os.getenv("SFTP_PASSWORD")

LOCAL_DIR = Path.home() / "anais_staging/input/staging"
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

MAPPING_FILE = LOCAL_DIR / "_source_filenames.json"

FILES_TO_FETCH = [
    ("/SCN_BDD/SIICEA/SIICEA", "SIICEA_GROUPECIBLES_SCN_", "sa_siicea_cibles.csv.gpg", ".gpg"),
    ("/SCN_BDD/SIICEA/SIICEA", "SIICEA_DECISIONS_SCN_", "sa_siicea_decisions.csv.gpg", ".gpg"),
    ("/SCN_BDD/SIREC", "sirec_", "sa_sirec.csv", ".csv"),
    ("/SCN_BDD/SIVSS", "SIVSS_SCN_", "sa_sivss.csv.gpg", ".gpg"),
    ("/SCN_BDD/SIICEA/SIICEA", "SIICEA_MISSIONSPREV_SCN_", "sa_siicea_missions_prog.csv.gpg", ".gpg"),
    ("/SCN_BDD/SIICEA/SIICEA", "SIICEA_MISSIONSREAL_SCN_", "sa_siicea_missions_real.csv.gpg", ".gpg"),
]


def connect():
    if not HOST:
        raise RuntimeError("SFTP_HOST is not defined")
    if not USERNAME:
        raise RuntimeError("SFTP_USERNAME is not defined")
    if not PASSWORD:
        raise RuntimeError("SFTP_PASSWORD is not defined")

    transport = Transport((HOST, PORT))
    transport.connect(username=USERNAME, password=PASSWORD)
    return SFTPClient.from_transport(transport)


def get_latest_file(sftp, remote_dir, keyword, required_suffix):
    files = sftp.listdir_attr(remote_dir)

    candidates = [
        f for f in files
        if keyword in f.filename and f.filename.endswith(required_suffix)
    ]

    if not candidates:
        raise RuntimeError(
            f"Aucun fichier trouve pour keyword={keyword} suffix={required_suffix} dans {remote_dir}"
        )

    latest = max(candidates, key=lambda x: x.st_mtime)
    return latest.filename


def save_mapping(mapping: dict):
    with open(MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)

    print(f"Mapping saved: {MAPPING_FILE}")


def main():
    print("Connexion SFTP...")
    sftp = connect()
    filename_mapping = {}

    try:
        for remote_dir, keyword, local_filename, required_suffix in FILES_TO_FETCH:
            latest_file = get_latest_file(sftp, remote_dir, keyword, required_suffix)

            remote_path = f"{remote_dir}/{latest_file}"
            local_path = LOCAL_DIR / local_filename

            print(f"Download {remote_path} -> {local_path}")
            sftp.get(remote_path, str(local_path))

            filename_mapping[local_filename] = latest_file

        save_mapping(filename_mapping)
        print("Telechargement termine")

    finally:
        sftp.close()


if __name__ == "__main__":
    main()