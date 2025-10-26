#!/usr/bin/env python3
"""
Local Helios Pipeline with SFTP Upload Support

This script extends the local Helios pipeline to optionally upload output files to SFTP
after running transformations. It combines functionality from both anais_project_pipeline
(SFTP upload) and local_project_pipeline (DuckDB processing).

Usage:
    # Without SFTP (manual export only)
    uv run run_local_with_sftp.py --env "local" --profile "Helios"

    # With SFTP (automatic upload)
    uv run run_local_with_sftp.py --env "local" --profile "Helios" --use-sftp
"""

# === Packages ===
import argparse
import os
from logging import Logger
from dotenv import load_dotenv
from paramiko import Transport, SFTPClient, RSAKey, Ed25519Key, ECDSAKey
from typing import Optional
from datetime import date

# === Modules ===
from pipeline.utils.config import setup_config
from pipeline.utils.load_yml import load_metadata_YAML
from pipeline.utils.logging_management import setup_logger
from pipeline.utils.sftp_sync import SFTPSync
from pipeline.database_management.duckdb_pipeline import DuckDBPipeline
from pipeline.utils.dbt_tools import dbt_exec

# === Constants ===
ENV_CHOICE = ["local"]  # Only local environment supported
PROFILE_CHOICE = ["Helios", "CertDC", "InspectionControlePA", "InspectionControlePH", "MatricePreciblage"]
METADATA_YML = "metadata.yml"
PROFILE_YML = "profiles.yml"


# === Custom SFTP Class with Private Key Support ===
class SFTPSyncWithKey(SFTPSync):
    """
    Extended SFTPSync class that supports private key authentication.

    Inherits from pipeline.utils.sftp_sync.SFTPSync and overrides the connect() method
    to support both password and private key authentication.
    """

    def __init__(self, output_folder: str, logger: Logger):
        """Initialize SFTP connection with support for private key."""
        super().__init__(output_folder, logger)
        # Load private key path from .env
        self.private_key_path = os.getenv("SFTP_PRIVATE_KEY_PATH")
        self.private_key_passphrase = os.getenv("SFTP_PRIVATE_KEY_PASSPHRASE")

    def _load_private_key(self, key_path: str, passphrase: Optional[str] = None):
        """
        Load private key from file, trying different key types.

        Parameters
        ----------
        key_path : str
            Path to private key file
        passphrase : Optional[str]
            Passphrase for encrypted key (optional)

        Returns
        -------
        paramiko.PKey
            Loaded private key
        """
        # Expand user path (~/)
        key_path = os.path.expanduser(key_path)

        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Private key file not found: {key_path}")

        # Try different key types
        key_types = [
            (RSAKey, "RSA"),
            (Ed25519Key, "Ed25519"),
            (ECDSAKey, "ECDSA"),
        ]

        for key_class, key_name in key_types:
            try:
                self.logger.info(f"Trying to load {key_name} private key from {key_path}")
                if passphrase:
                    return key_class.from_private_key_file(key_path, password=passphrase)
                else:
                    return key_class.from_private_key_file(key_path)
            except Exception as e:
                self.logger.debug(f"Failed to load as {key_name}: {e}")
                continue

        raise ValueError(f"Could not load private key from {key_path}. Tried RSA, Ed25519, and ECDSA formats.")

    def connect(self):
        """
        Initialize SFTP connection using private key (if provided) or password.

        Priority:
        1. Private key authentication (if SFTP_PRIVATE_KEY_PATH is set)
        2. Password authentication (if SFTP_PASSWORD is set)
        """
        try:
            self.transport = Transport((self.host, self.port))

            # Try private key authentication first
            if self.private_key_path:
                self.logger.info("Connecting with private key authentication...")
                try:
                    private_key = self._load_private_key(
                        self.private_key_path,
                        self.private_key_passphrase
                    )
                    self.transport.connect(username=self.username, pkey=private_key)
                    self.sftp = SFTPClient.from_transport(self.transport)
                    self.logger.info("✅ SFTP connection established with private key")
                    return
                except Exception as e:
                    self.logger.error(f"Private key authentication failed: {e}")
                    raise

            # Fallback to password authentication
            elif self.password:
                self.logger.info("Connecting with password authentication...")
                self.transport.connect(username=self.username, password=self.password)
                self.sftp = SFTPClient.from_transport(self.transport)
                self.logger.info("✅ SFTP connection established with password")

            else:
                raise ValueError(
                    "No authentication method available. "
                    "Please set either SFTP_PRIVATE_KEY_PATH or SFTP_PASSWORD in .env file."
                )

        except Exception as e:
            self.logger.error(f"❌ SFTP connection failed: {e}")
            raise


def local_helios_pipeline_with_sftp(
    profile: str,
    config: dict,
    db_config: dict,
    staging_db_config: dict,
    today: str,
    logger: Logger,
    use_sftp: bool = False
):
    """
    Pipeline for Helios in local environment with optional SFTP upload.

    Steps:
        1. Connect to Staging DuckDB and copy required tables
        2. Connect to Helios DuckDB
        3. Run DBT transformations
        4. Export views as CSV files
        5. (Optional) Upload files to SFTP

    Parameters
    ----------
    profile : str
        DBT profile to use from 'profiles.yml'.
    config : dict
        Profile metadata (from metadata.yml).
    db_config : dict
        Helios DuckDB configuration parameters (from 'profiles.yml').
    staging_db_config : dict
        Staging DuckDB configuration parameters (from 'profiles.yml').
    today : str
        Today's date (format YYYY_MM_DD) used in output filenames.
    logger : Logger
        Log file.
    use_sftp : bool
        If True, upload files to SFTP after export.
    """
    # Step 1: Initialize DuckDB loader
    logger.info("=" * 80)
    logger.info("🦆 STEP 1: Initializing DuckDB Helios connection...")
    logger.info("=" * 80)

    ddb_loader = DuckDBPipeline(
        db_config=db_config,
        config=config,
        logger=logger,
        staging_db_config=staging_db_config
    )

    # Step 2: Copy tables from Staging database
    logger.info("=" * 80)
    logger.info("📊 STEP 2: Copying tables from Staging database...")
    logger.info("=" * 80)

    ddb_loader.connect()

    try:
        # Check if Staging database exists
        if os.path.isfile(staging_db_config["path"]):
            logger.info(f"Found Staging database: {staging_db_config['path']}")
            logger.info(f"Copying {len(config['table_to_copy'])} tables...")
            ddb_loader.copy_table(config["table_to_copy"])
            logger.info("✅ Tables copied successfully")
            logger.info("")

        elif os.listdir(config["local_directory_input"]) and os.listdir(config["create_table_directory"]):
            logger.info("Staging database not found, loading from CSV files...")
            ddb_loader.run()
            logger.info("✅ Data loaded from CSV files")
            logger.info("")
        else:
            logger.error(
                "❌ Cannot populate Helios DuckDB database.\n"
                f"- Staging DuckDB not found at: {staging_db_config['path']}\n"
                f"- OR empty directories:\n"
                f"    > CSV files: {config['local_directory_input']}\n"
                f"    > SQL schemas: {config['create_table_directory']}"
            )
            raise FileNotFoundError("No data source available for Helios pipeline")
    finally:
        duckdb_empty = ddb_loader.is_duckdb_empty()
        ddb_loader.close()

    # Step 3: Run DBT transformations (if database is not empty)
    if not duckdb_empty:
        logger.info("=" * 80)
        logger.info("🔄 STEP 3: Running DBT transformations...")
        logger.info("=" * 80)

        # Create views
        dbt_exec("run", profile, "local", config["models_directory"], ".", logger)

        # Run tests
        dbt_exec("test", profile, "local", config["models_directory"], ".", logger)

        logger.info("✅ DBT transformations complete")
        logger.info("")

        # Step 4: Export views as CSV files
        logger.info("=" * 80)
        logger.info("📤 STEP 4: Exporting views to CSV files...")
        logger.info("=" * 80)

        ddb_loader.connect()
        ddb_loader.export_csv(config["files_to_upload"], date=today)
        ddb_loader.close()

        logger.info(f"✅ Exported {len(config['files_to_upload'])} CSV files to {config['local_directory_output']}")
        logger.info("")

        # Step 5: SFTP Upload (optional)
        if use_sftp:
            logger.info("=" * 80)
            logger.info("📤 STEP 5: Uploading files to SFTP...")
            logger.info("=" * 80)

            try:
                sftp = SFTPSyncWithKey(config["local_directory_output"], logger)
                sftp.upload_file_to_sftp(
                    config["files_to_upload"],
                    config["local_directory_output"],
                    config["remote_directory_output"],
                    date=today
                )
                logger.info("✅ SFTP upload complete!")
                logger.info("")
            except Exception as e:
                logger.error(f"❌ SFTP upload failed: {e}")
                logger.error("Make sure .env file contains SFTP credentials:")
                logger.error("  Required: SFTP_HOST, SFTP_PORT, SFTP_USERNAME")
                logger.error("  Authentication: SFTP_PRIVATE_KEY_PATH or SFTP_PASSWORD")
                logger.error("  Optional: SFTP_PRIVATE_KEY_PASSPHRASE (if key is encrypted)")
                raise
        else:
            logger.info("=" * 80)
            logger.info("📂 STEP 5: SFTP upload skipped (files saved locally)")
            logger.info("=" * 80)
            logger.info(f"Output files available in: {config['local_directory_output']}")
            logger.info("")

        # Final summary
        logger.info("=" * 80)
        logger.info("✅ Pipeline completed successfully!")
        logger.info("=" * 80)
        logger.info(f"Database: {db_config['path']}")
        logger.info(f"Output: {config['local_directory_output']}")
        if use_sftp:
            logger.info(f"SFTP: Uploaded to {config['remote_directory_output']}")
        logger.info("")

    else:
        logger.error(f"❌ Database {db_config['path']} is empty")
        raise RuntimeError("Helios DuckDB database is empty after loading")


def main():
    """Main execution function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Local Helios Pipeline with optional SFTP upload"
    )
    parser.add_argument(
        "--env",
        choices=ENV_CHOICE,
        default=ENV_CHOICE[0],
        help="Execution environment (only 'local' supported)"
    )
    parser.add_argument(
        "--profile",
        choices=PROFILE_CHOICE,
        default=PROFILE_CHOICE[0],
        help="DBT profile to execute"
    )
    parser.add_argument(
        "--use-sftp",
        action="store_true",
        help="Upload output files to SFTP after export (requires .env with SFTP credentials)"
    )
    args = parser.parse_args()

    # Setup configuration
    logger = setup_logger(args.env, f"logs/log_{args.env}_sftp.log")
    config = load_metadata_YAML(METADATA_YML, args.profile, logger, ".")
    db_config = load_metadata_YAML(PROFILE_YML, args.profile, logger, ".")["outputs"][args.env]
    staging_db_config = load_metadata_YAML(PROFILE_YML, "Staging", logger, ".")["outputs"][args.env]
    today = date.strftime(date.today(), "%Y_%m_%d")

    # Print execution info
    logger.info("=" * 80)
    logger.info("🚀 LOCAL HELIOS PIPELINE WITH SFTP SUPPORT")
    logger.info("=" * 80)
    logger.info(f"Environment: {args.env}")
    logger.info(f"Profile: {args.profile}")
    logger.info(f"SFTP Upload: {'✅ Enabled' if args.use_sftp else '❌ Disabled (local export only)'}")
    logger.info(f"Database: {db_config['path']}")
    logger.info(f"Date: {today}")
    logger.info("")

    # Run pipeline
    local_helios_pipeline_with_sftp(
        profile=args.profile,
        config=config,
        db_config=db_config,
        staging_db_config=staging_db_config,
        today=today,
        logger=logger,
        use_sftp=args.use_sftp
    )


if __name__ == "__main__":
    main()
