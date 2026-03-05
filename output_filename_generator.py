#!/usr/bin/env python3
"""
Output Filename Generator for ANAIS Helios Pipeline

Queries input file dates from Staging database and generates output filenames
with embedded dates from source files.
"""

import duckdb
from datetime import datetime, date
from typing import Dict, Optional
import logging
import os

logger = logging.getLogger(__name__)


class OutputFilenameGenerator:
    """
    Generate output filenames based on input file dates from Staging database.

    This class queries the input_files_date table created by the Staging pipeline
    and uses those dates to create meaningful output filenames.
    """

    # Mapping of Helios view names to file types
    VIEW_TO_FILE_TYPE = {
        'helios__sivss': 'sivss',
        'helios__sirec': 'sirec',
        'helios__missions': 'siicea_missions_real',
    }

    # Default filename prefixes
    DEFAULT_PREFIXES = {
        'helios__sivss': 'sivss',
        'helios__sirec': 'sirec',
        'helios__missions': 'siicea',
    }

    def __init__(self, staging_db_path: str, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize output filename generator.

        Args:
            staging_db_path: Path to Staging DuckDB database
            logger_instance: Optional logger instance
        """
        self.staging_db_path = staging_db_path
        self.logger = logger_instance or logger
        self.file_dates: Dict[str, date] = {}

    def query_input_file_dates(self) -> Dict[str, date]:
        """
        Query input file dates from Staging database.

        Returns:
            Dict mapping file_type to extracted_date

        Example:
            {
                'sivss': datetime.date(2025, 10, 7),
                'sirec': datetime.date(2025, 10, 26),
                'siicea_missions_real': datetime.date(2025, 10, 20)
            }
        """
        if not os.path.exists(self.staging_db_path):
            self.logger.warning(f"⚠️  Staging database not found: {self.staging_db_path}")
            return {}

        try:
            # Connect to Staging database (read-only)
            conn = duckdb.connect(self.staging_db_path, read_only=True)

            # Check if table exists
            table_exists = conn.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'input_files_date'
            """).fetchone()[0]

            if not table_exists:
                self.logger.warning("⚠️  input_files_date table not found in Staging database")
                conn.close()
                return {}

            # Query latest dates for each file type
            results = conn.execute("""
                WITH RankedDates AS (
                    SELECT
                        file_type,
                        extracted_date,
                        ingestion_timestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY file_type
                            ORDER BY ingestion_timestamp DESC
                        ) as rn
                    FROM input_files_date
                    WHERE file_type IN ('sivss', 'sirec', 'siicea_missions_real')
                )
                SELECT file_type, extracted_date
                FROM RankedDates
                WHERE rn = 1
            """).fetchall()

            # Convert to dict
            file_dates = {row[0]: row[1] for row in results}

            # Log what we found
            self.logger.info("📅 Input file dates from Staging:")
            for file_type, extracted_date in file_dates.items():
                self.logger.info(f"   {file_type}: {extracted_date}")

            conn.close()
            return file_dates

        except Exception as e:
            self.logger.error(f"❌ Failed to query input file dates: {e}")
            return {}

    def generate_filenames(self, use_current_date_fallback: bool = True) -> Dict[str, str]:
        """
        Generate output filenames based on input file dates.

        Args:
            use_current_date_fallback: If True, use current date when input date not found

        Returns:
            Dict mapping view_name to output_filename

        Example:
            {
                'helios__sivss': 'sivss_20251007.csv',
                'helios__sirec': 'sirec_20251026.csv',
                'helios__missions': 'siicea_20251020.csv'
            }
        """
        self.logger.info("=" * 80)
        self.logger.info("📝 Generating output filenames from input file dates...")
        self.logger.info("=" * 80)

        # Query dates from Staging database
        self.file_dates = self.query_input_file_dates()

        output_filenames = {}

        for view_name, file_type in self.VIEW_TO_FILE_TYPE.items():
            prefix = self.DEFAULT_PREFIXES[view_name]

            # Get date for this file type
            extracted_date = self.file_dates.get(file_type)

            if extracted_date:
                # Use extracted date from input file
                date_str = extracted_date.strftime('%Y%m%d')
                filename = f"{prefix}_{date_str}.csv"
                self.logger.info(f"✅ {view_name} → {filename} (from input file date)")
            elif use_current_date_fallback:
                # Fallback to current date
                date_str = datetime.now().strftime('%Y%m%d')
                filename = f"{prefix}_{date_str}.csv"
                self.logger.warning(f"⚠️  {view_name} → {filename} (fallback: no input date found)")
            else:
                # No fallback - use default naming
                date_str = datetime.now().strftime('%Y_%m_%d')
                filename = f"test_{view_name}_{date_str}.csv"
                self.logger.warning(f"⚠️  {view_name} → {filename} (default: no input date)")

            output_filenames[view_name] = filename

        self.logger.info("=" * 80)
        return output_filenames

    def get_filename_for_view(self, view_name: str, fallback_to_current: bool = True) -> str:
        """
        Get output filename for a specific view.

        Args:
            view_name: DBT view name (e.g., 'helios__sivss')
            fallback_to_current: If True, use current date when input date not found

        Returns:
            Output filename string
        """
        if not self.file_dates:
            # Lazy load if not already queried
            self.file_dates = self.query_input_file_dates()

        file_type = self.VIEW_TO_FILE_TYPE.get(view_name)
        prefix = self.DEFAULT_PREFIXES.get(view_name, view_name)

        if not file_type:
            # Unknown view - use current date
            date_str = datetime.now().strftime('%Y%m%d')
            return f"{prefix}_{date_str}.csv"

        extracted_date = self.file_dates.get(file_type)

        if extracted_date:
            date_str = extracted_date.strftime('%Y%m%d')
            return f"{prefix}_{date_str}.csv"
        elif fallback_to_current:
            date_str = datetime.now().strftime('%Y%m%d')
            return f"{prefix}_{date_str}.csv"
        else:
            date_str = datetime.now().strftime('%Y_%m_%d')
            return f"test_{prefix}_{date_str}.csv"

    def print_mapping_summary(self):
        """Print summary of view name to filename mapping."""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("📋 Output Filename Mapping Summary")
        self.logger.info("=" * 80)

        filenames = self.generate_filenames()

        for view_name, filename in filenames.items():
            file_type = self.VIEW_TO_FILE_TYPE.get(view_name, 'unknown')
            extracted_date = self.file_dates.get(file_type)

            if extracted_date:
                self.logger.info(f"  {view_name:20} → {filename:30} (input date: {extracted_date})")
            else:
                self.logger.info(f"  {view_name:20} → {filename:30} (no input date)")

        self.logger.info("=" * 80)


if __name__ == "__main__":
    # Test the generator
    logging.basicConfig(level=logging.INFO)

    print("\n=== Testing OutputFilenameGenerator ===\n")

    # Test with mock database
    staging_db_path = "../anais_staging/data/staging/duckdb_database.duckdb"

    if os.path.exists(staging_db_path):
        generator = OutputFilenameGenerator(staging_db_path, logger)

        # Generate filenames
        filenames = generator.generate_filenames()

        print("\n--- Generated Filenames ---")
        for view_name, filename in filenames.items():
            print(f"  {view_name}: {filename}")

        # Print summary
        generator.print_mapping_summary()

    else:
        print(f"Staging database not found at: {staging_db_path}")
        print("\nTesting with fallback (current date):")

        generator = OutputFilenameGenerator("/nonexistent/path.duckdb", logger)
        filenames = generator.generate_filenames()

        for view_name, filename in filenames.items():
            print(f"  {view_name}: {filename}")

    print("\n✅ OutputFilenameGenerator test complete")
