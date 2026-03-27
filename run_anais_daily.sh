
#!/bin/bash

set -Eeuo pipefail

trap 'echo "ERROR at line $LINENO - command: $BASH_COMMAND"' ERR

run_with_retry() {
  local max_attempts=3
  local attempt=1

  until "$@"; do
    echo "Attempt $attempt failed: $*"
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo "All attempts failed: $*"
      return 1
    fi
    attempt=$((attempt + 1))
    echo "Retrying in 10 seconds..."
    sleep 10
  done
}

LOG_DIR="$HOME/logs/anais"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/anais_$(date +%Y%m%d).log"

exec >> "$LOG_FILE" 2>&1

export HOME="/home/thoude"
export PATH="/home/thoude/.local/bin:/usr/local/bin:/usr/bin:/bin"

STAGING_DIR="$HOME/anais_staging"
HELIOS_DIR="$HOME/anais_helios"

mkdir -p "$LOG_DIR"

exec >> "$LOG_DIR/anais_daily_cron.log" 2>&1

echo "============================================================"
echo "$(date '+%Y-%m-%d %H:%M:%S') START anais daily pipeline"
echo "============================================================"

# Charge le profil si necessaire pour uv / variables d'environnement
if [ -f "$HOME/.profile" ]; then
  . "$HOME/.profile"
fi

# Charge le .env
set -a
source "$STAGING_DIR/.env"
set +a

# Nettoyage du dossier input
mkdir -p "$STAGING_DIR/input/staging"
find "$STAGING_DIR/input/staging" -maxdepth 1 -type f \( -name '*.gpg' -o -name '*.csv' \) -delete

# Téléchargement SFTP
cd "$STAGING_DIR"
run_with_retry uv run python scripts/fetch_latest_sftp_inputs.py

# Déchiffrage
decrypt_files() {
	find "$STAGING_DIR/input/staging" -maxdepth 1 -type f -name '*.gpg' -print0 | while IFS= read -r -d '' file; do
  		output_file="${file%.gpg}"
  		lower_file="$(basename "$file" | tr '[:upper:]' '[:lower:]')"

  	echo "$(date '+%F %T') decrypt: $file -> $output_file"

  	if [[ "$lower_file" == *siicea* ]]; then
    		gpg --batch --yes --pinentry-mode loopback \
        	--passphrase "$GPG_PASSPHRASE_SIICEA" \
        	--output "$output_file" \
        	--decrypt "$file"
  	elif [[ "$lower_file" == *sivss* ]]; then
    	gpg --batch --yes --pinentry-mode loopback \
        --passphrase "$GPG_PASSPHRASE_SIVSS" \
        --output "$output_file" \
        --decrypt "$file"
  	else
    		echo "$(date '+%F %T') skip decrypt: unknown key for $file"
    		returnt 1
  	fi
	done
}

run_with_retry decrypt_files

# Etape 3 : execution de Staging
cd "$STAGING_DIR"
run_with_retry uv run run_local_with_sftp.py --env local --profile Staging

# Etape 4 : copie de la base staging vers Helios
mkdir -p "$HELIOS_DIR/data/staging"
cp -f "$STAGING_DIR/data/staging/duckdb_database.duckdb" \
      "$HELIOS_DIR/data/staging/duckdb_database.duckdb"

# Etape 5 : execution de Helios + redepot SFTP
cd "$HELIOS_DIR"
export PATH="$HOME/.local/bin:$PATH"
run_with_retry uv run run_local_with_sftp.py --env local --profile Helios --use-sftp

if [ $? -ne 0 ]; then
  echo " Pipeline failed" | mail -s "ANALYSE PIPELINE FAILED" houdetitouan@seenovate.com
fi

echo "============================================================"
echo "$(date '+%Y-%m-%d %H:%M:%S') END anais pipeline SUCCESS"
echo "============================================================"
