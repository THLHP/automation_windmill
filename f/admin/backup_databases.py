import os
import subprocess
import datetime
import sys
import shutil
import platform
from typing import TypedDict
import psycopg2
import wmill

class postgresql(TypedDict):
    host: str
    port: int
    user: str
    dbname: str
    sslmode: str
    password: str
    root_certificate_pem: str


def install_postgresql_client():
    """
    Install PostgreSQL client tools (pg_dump, pg_restore) if not available.
    Supports different operating systems and package managers.
    """
    print("Installing PostgreSQL client tools...")

    system = platform.system().lower()

    try:
        if system == "linux":
            # Try to detect the distribution
            try:
                with open("/etc/os-release", "r") as f:
                    os_release = f.read().lower()
            except:
                os_release = ""

            if "ubuntu" in os_release or "debian" in os_release:
                # Ubuntu/Debian
                print("Detected Ubuntu/Debian - installing via apt...")
                subprocess.run(["apt-get", "update"], check=True)
                subprocess.run(["apt-get", "install", "-y", "postgresql-client"], check=True)

            elif "alpine" in os_release:
                # Alpine Linux (common in Docker containers)
                print("Detected Alpine Linux - installing via apk...")
                subprocess.run(["apk", "update"], check=True)
                subprocess.run(["apk", "add", "postgresql-client"], check=True)

            elif "centos" in os_release or "rhel" in os_release or "fedora" in os_release:
                # CentOS/RHEL/Fedora
                print("Detected CentOS/RHEL/Fedora - installing via yum/dnf...")
                try:
                    subprocess.run(["dnf", "install", "-y", "postgresql"], check=True)
                except FileNotFoundError:
                    subprocess.run(["yum", "install", "-y", "postgresql"], check=True)
            else:
                # Generic Linux - try common package managers
                print("Unknown Linux distribution - trying common package managers...")
                for pkg_manager, install_cmd in [
                    ("apt-get", ["apt-get", "update", "&&", "apt-get", "install", "-y", "postgresql-client"]),
                    ("yum", ["yum", "install", "-y", "postgresql"]),
                    ("apk", ["apk", "add", "postgresql-client"]),
                ]:
                    if shutil.which(pkg_manager):
                        subprocess.run(install_cmd, check=True)
                        break

        elif system == "darwin":
            # macOS
            print("Detected macOS - installing via Homebrew...")
            if not shutil.which("brew"):
                raise RuntimeError("Homebrew not found. Please install Homebrew first.")
            subprocess.run(["brew", "install", "postgresql"], check=True)

        else:
            raise RuntimeError(f"Unsupported operating system: {system}")

        print("✓ PostgreSQL client tools installed successfully")

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to install PostgreSQL client tools: {e}")
    except Exception as e:
        raise RuntimeError(f"Error during installation: {e}")


def check_and_install_pg_dump():
    """
    Check if pg_dump is available, and install it if not.
    Returns the path to pg_dump.
    """
    # Check if pg_dump is already available
    pg_dump_path = shutil.which("pg_dump")

    if pg_dump_path:
        print(f"✓ pg_dump found at: {pg_dump_path}")
        return pg_dump_path

    print("pg_dump not found in PATH. Installing PostgreSQL client tools...")

    try:
        install_postgresql_client()

        # Check again after installation
        pg_dump_path = shutil.which("pg_dump")
        if pg_dump_path:
            print(f"✓ pg_dump installed and found at: {pg_dump_path}")
            return pg_dump_path
        else:
            # Sometimes the PATH needs to be updated after installation
            # Common PostgreSQL installation paths
            common_paths = [
                "/usr/bin/pg_dump",
                "/usr/local/bin/pg_dump",
                "/opt/homebrew/bin/pg_dump",
                "/usr/pgsql-*/bin/pg_dump",
            ]

            for path in common_paths:
                if os.path.exists(path):
                    print(f"✓ pg_dump found at: {path}")
                    return path

            raise RuntimeError("pg_dump still not found after installation")

    except Exception as e:
        raise RuntimeError(f"Failed to install or locate pg_dump: {e}")


def main(database_credentials: postgresql, backup_directory: str):
    """
    Backup all databases from a PostgreSQL server.

    Args:
        database_credentials: PostgreSQL connection credentials
        backup_directory: Directory path where backup files will be stored

    Returns:
        Dict with backup results and file paths
    """

    # Check and install pg_dump if needed
    try:
        pg_dump_path = check_and_install_pg_dump()
    except Exception as e:
        return {
            'error': f"Failed to setup pg_dump: {str(e)}",
            'status': 'failed',
            'successful_backups': [],
            'failed_backups': [],
            'total_databases': 0
        }

    # Extract connection parameters
    host = database_credentials['host']
    port = database_credentials.get('port', 5432)
    user = database_credentials['user']
    password = database_credentials['password']
    initial_db = database_credentials.get('dbname', 'postgres')  # Default to postgres db for listing

    # Create backup directory if it doesn't exist
    os.makedirs(backup_directory, exist_ok=True)

    # Generate timestamp for backup files
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    backup_results = {
        'timestamp': timestamp,
        'successful_backups': [],
        'failed_backups': [],
        'total_databases': 0,
        'backup_directory': backup_directory
    }

    try:
        # Connect to PostgreSQL to get list of databases
        conn_string = f"host='{host}' port='{port}' dbname='{initial_db}' user='{user}' password='{password}'"
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # Get list of all databases (excluding system templates)
        cursor.execute("""
            SELECT datname FROM pg_database
            WHERE datistemplate = false
            AND datname NOT IN ('postgres', 'template0', 'template1')
            ORDER BY datname;
        """)

        databases = [row[0] for row in cursor.fetchall()]
        backup_results['total_databases'] = len(databases)

        print(f"Found {len(databases)} databases to backup: {', '.join(databases)}")

        # Close the connection
        cursor.close()
        conn.close()

        # Backup each database
        for db_name in databases:
            try:
                backup_file = os.path.join(backup_directory, f"{db_name}_{timestamp}.sql")

                # Use pg_dump to create backup
                # Set PGPASSWORD environment variable for authentication
                env = os.environ.copy()
                env['PGPASSWORD'] = password

                cmd = [
                    pg_dump_path,  # Use the discovered pg_dump path
                    '-h', host,
                    '-p', str(port),
                    '-U', user,
                    '-d', db_name,
                    '--verbose',
                    '--no-password',
                    '--format=custom',  # Use custom format for better compression and faster restore
                    '--file', backup_file
                ]

                print(f"Starting backup of database: {db_name}")
                result = subprocess.run(cmd, env=env, capture_output=True, text=True)

                if result.returncode == 0:
                    # Get file size for reporting
                    file_size = os.path.getsize(backup_file)
                    file_size_mb = round(file_size / (1024 * 1024), 2)

                    backup_info = {
                        'database': db_name,
                        'file_path': backup_file,
                        'file_size_mb': file_size_mb,
                        'status': 'success'
                    }
                    backup_results['successful_backups'].append(backup_info)
                    print(f" Successfully backed up {db_name} ({file_size_mb} MB)")

                else:
                    error_info = {
                        'database': db_name,
                        'error': result.stderr,
                        'status': 'failed'
                    }
                    backup_results['failed_backups'].append(error_info)
                    print(f" Failed to backup {db_name}: {result.stderr}")

                    # Remove failed backup file if it exists
                    if os.path.exists(backup_file):
                        os.remove(backup_file)

            except Exception as e:
                error_info = {
                    'database': db_name,
                    'error': str(e),
                    'status': 'failed'
                }
                backup_results['failed_backups'].append(error_info)
                print(f" Exception while backing up {db_name}: {str(e)}")

        # Summary
        successful_count = len(backup_results['successful_backups'])
        failed_count = len(backup_results['failed_backups'])

        print(f"\n=== Backup Summary ===")
        print(f"Total databases: {backup_results['total_databases']}")
        print(f"Successful backups: {successful_count}")
        print(f"Failed backups: {failed_count}")
        print(f"Backup directory: {backup_directory}")

        if successful_count > 0:
            total_size = sum(backup['file_size_mb'] for backup in backup_results['successful_backups'])
            print(f"Total backup size: {total_size:.2f} MB")

        return backup_results

    except psycopg2.Error as e:
        error_msg = f"Database connection error: {str(e)}"
        print(f" {error_msg}")
        backup_results['connection_error'] = error_msg
        return backup_results

    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f" {error_msg}")
        backup_results['unexpected_error'] = error_msg
        return backup_results