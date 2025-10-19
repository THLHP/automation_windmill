import os
import subprocess
import datetime
import shutil
import platform
import glob
from typing import TypedDict
import psycopg2
from wmill import set_progress

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
    Install PostgreSQL 17 client tools (pg_dump, pg_restore) specifically.
    Supports different operating systems and package managers.
    """
    print("Installing PostgreSQL 17 client tools...")

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
                # Ubuntu/Debian - Install PostgreSQL 17 specifically
                print("Detected Ubuntu/Debian - installing PostgreSQL 17 client...")

                # Get the distribution codename
                result = subprocess.run(["lsb_release", "-cs"], capture_output=True, text=True, check=True)
                distro_codename = result.stdout.strip()
                print(f"Detected distribution: {distro_codename}")

                # Install prerequisites
                subprocess.run(["apt-get", "update"], check=True)
                subprocess.run(["apt-get", "install", "-y", "wget", "ca-certificates", "gnupg", "lsb-release"], check=True)

                # Add PostgreSQL signing key using the modern method
                subprocess.run(["wget", "--quiet", "-O", "/tmp/pgdg.asc", "https://www.postgresql.org/media/keys/ACCC4CF8.asc"], check=True)
                subprocess.run(["gpg", "--dearmor", "-o", "/usr/share/keyrings/postgresql-keyring.gpg", "/tmp/pgdg.asc"], check=True)
                subprocess.run(["rm", "/tmp/pgdg.asc"], check=True)

                # Add PostgreSQL repository with proper keyring
                repo_line = f"deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt {distro_codename}-pgdg main"
                subprocess.run(["sh", "-c", f"echo '{repo_line}' > /etc/apt/sources.list.d/pgdg.list"], check=True)

                # Update and install PostgreSQL 17 client
                subprocess.run(["apt-get", "update"], check=True)
                subprocess.run(["apt-get", "install", "-y", "postgresql-client-17"], check=True)

            elif "alpine" in os_release:
                # Alpine Linux - Install PostgreSQL 17
                print("Detected Alpine Linux - installing PostgreSQL 17 client...")
                subprocess.run(["apk", "update"], check=True)
                # Try to install specific version, fallback to latest
                try:
                    subprocess.run(["apk", "add", "postgresql17-client"], check=True)
                except subprocess.CalledProcessError:
                    print("PostgreSQL 17 not available, installing latest...")
                    subprocess.run(["apk", "add", "postgresql-client"], check=True)

            elif "centos" in os_release or "rhel" in os_release:
                # CentOS/RHEL - Install PostgreSQL 17
                print("Detected CentOS/RHEL - installing PostgreSQL 17 client...")
                # Install PostgreSQL 17 repository
                subprocess.run(["yum", "install", "-y", "https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm"], check=True)
                subprocess.run(["yum", "install", "-y", "postgresql17"], check=True)

            elif "fedora" in os_release:
                # Fedora - Install PostgreSQL 17
                print("Detected Fedora - installing PostgreSQL 17 client...")
                subprocess.run(["dnf", "install", "-y", "https://download.postgresql.org/pub/repos/yum/reporpms/F-39-x86_64/pgdg-fedora-repo-latest.noarch.rpm"], check=True)
                subprocess.run(["dnf", "install", "-y", "postgresql17"], check=True)

            else:
                # Generic Linux - try PostgreSQL 17 first, fallback to generic
                print("Unknown Linux distribution - trying PostgreSQL 17...")
                # Try different package managers in order
                if shutil.which("apt-get"):
                    print("Trying APT package manager...")
                    try:
                        # Try to set up PostgreSQL 17 repository for apt-based systems
                        subprocess.run(["apt-get", "update"], check=True)
                        subprocess.run(["apt-get", "install", "-y", "wget", "ca-certificates", "gnupg", "lsb-release"], check=True)

                        # Get distribution codename if available
                        try:
                            result = subprocess.run(["lsb_release", "-cs"], capture_output=True, text=True, check=True)
                            distro_codename = result.stdout.strip()

                            # Add PostgreSQL repository with modern keyring method
                            subprocess.run(["wget", "--quiet", "-O", "/tmp/pgdg.asc", "https://www.postgresql.org/media/keys/ACCC4CF8.asc"], check=True)
                            subprocess.run(["gpg", "--dearmor", "-o", "/usr/share/keyrings/postgresql-keyring.gpg", "/tmp/pgdg.asc"], check=True)
                            subprocess.run(["rm", "/tmp/pgdg.asc"], check=True)

                            repo_line = f"deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt {distro_codename}-pgdg main"
                            subprocess.run(["sh", "-c", f"echo '{repo_line}' > /etc/apt/sources.list.d/pgdg.list"], check=True)
                            subprocess.run(["apt-get", "update"], check=True)
                            subprocess.run(["apt-get", "install", "-y", "postgresql-client-17"], check=True)
                        except subprocess.CalledProcessError:
                            print("Failed to set up PostgreSQL 17 repository, installing generic version...")
                            subprocess.run(["apt-get", "install", "-y", "postgresql-client"], check=True)

                    except subprocess.CalledProcessError:
                        print("APT installation failed")

                elif shutil.which("dnf"):
                    print("Trying DNF package manager...")
                    try:
                        subprocess.run(["dnf", "install", "-y", "postgresql17"], check=True)
                    except subprocess.CalledProcessError:
                        print("PostgreSQL 17 not available, installing generic version...")
                        subprocess.run(["dnf", "install", "-y", "postgresql"], check=True)

                elif shutil.which("yum"):
                    print("Trying YUM package manager...")
                    try:
                        subprocess.run(["yum", "install", "-y", "postgresql17"], check=True)
                    except subprocess.CalledProcessError:
                        print("PostgreSQL 17 not available, installing generic version...")
                        subprocess.run(["yum", "install", "-y", "postgresql"], check=True)

                elif shutil.which("apk"):
                    print("Trying APK package manager...")
                    try:
                        subprocess.run(["apk", "update"], check=True)
                        subprocess.run(["apk", "add", "postgresql17-client"], check=True)
                    except subprocess.CalledProcessError:
                        print("PostgreSQL 17 not available, installing generic version...")
                        subprocess.run(["apk", "add", "postgresql-client"], check=True)

                else:
                    raise RuntimeError("No supported package manager found (apt-get, dnf, yum, apk)")

        elif system == "darwin":
            # macOS - Install PostgreSQL 17 specifically
            print("Detected macOS - installing PostgreSQL 17 via Homebrew...")
            if not shutil.which("brew"):
                raise RuntimeError("Homebrew not found. Please install Homebrew first.")
            # Install specific version 17
            try:
                subprocess.run(["brew", "install", "postgresql@17"], check=True)
                # Link the version 17 binaries
                subprocess.run(["brew", "link", "postgresql@17", "--force"], check=True)
            except subprocess.CalledProcessError:
                print("PostgreSQL 17 not available, installing latest...")
                subprocess.run(["brew", "install", "postgresql"], check=True)

        else:
            raise RuntimeError(f"Unsupported operating system: {system}")

        print("✓ PostgreSQL 17 client tools installed successfully")

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to install PostgreSQL 17 client tools: {e}")
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
            # PostgreSQL 17 specific paths and common installation paths
            common_paths = [
                "/usr/bin/pg_dump",
                "/usr/local/bin/pg_dump",
                "/opt/homebrew/bin/pg_dump",
                "/opt/homebrew/Cellar/postgresql@17/*/bin/pg_dump",  # Homebrew PostgreSQL 17
                "/usr/pgsql-17/bin/pg_dump",  # RHEL/CentOS PostgreSQL 17
                "/usr/lib/postgresql/17/bin/pg_dump",  # Ubuntu/Debian PostgreSQL 17
                "/usr/pgsql-*/bin/pg_dump",  # Generic PostgreSQL versions
            ]

            # For paths with wildcards, use glob to expand them
            for path_pattern in common_paths:
                if '*' in path_pattern:
                    # Expand wildcard paths
                    expanded_paths = glob.glob(path_pattern)
                    for expanded_path in expanded_paths:
                        if os.path.exists(expanded_path):
                            print(f"✓ pg_dump found at: {expanded_path}")
                            return expanded_path
                else:
                    # Direct path check
                    if os.path.exists(path_pattern):
                        print(f"✓ pg_dump found at: {path_pattern}")
                        return path_pattern

            raise RuntimeError("pg_dump still not found after installation")

    except Exception as e:
        raise RuntimeError(f"Failed to install or locate pg_dump: {e}")


def main(database_credentials: postgresql, backup_directory: str):
    """
    Backup all databases from a PostgreSQL server using PostgreSQL 17 client tools.

    This function automatically installs PostgreSQL 17 client tools (pg_dump) if not available,
    ensuring version compatibility with PostgreSQL 17 servers. Creates a timestamped subdirectory
    within the backup directory for organized storage.

    Args:
        database_credentials: PostgreSQL connection credentials
        backup_directory: Base directory path where timestamped backup folder will be created

    Returns:
        Dict with backup results, file paths, and directory information

    Directory Structure:
        backup_directory/
        └── 2025-05-19_14-30-25/
            ├── database1_20250519_143025.sql
            ├── database2_20250519_143025.sql
            └── database3_20250519_143025.sql
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

    # Generate timestamps
    now = datetime.datetime.now()
    # Human-readable timestamp for directory name (2025-05-19_00-00-00)
    dir_timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    # Compact timestamp for file names (20250519_000000)
    file_timestamp = now.strftime("%Y%m%d_%H%M%S")

    # Create timestamped backup directory
    timestamped_backup_dir = os.path.join(backup_directory, dir_timestamp)
    os.makedirs(timestamped_backup_dir, exist_ok=True)

    backup_results = {
        'timestamp': file_timestamp,
        'directory_timestamp': dir_timestamp,
        'successful_backups': [],
        'failed_backups': [],
        'total_databases': 0,
        'backup_directory': backup_directory,
        'timestamped_backup_directory': timestamped_backup_dir
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

        # Backup each database with progress tracking
        total_databases = len(databases)
        for i, db_name in enumerate(databases, 1):
            try:
                backup_file = os.path.join(timestamped_backup_dir, f"{db_name}_{file_timestamp}.sql")

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

                # Update progress - starting backup for this database
                progress_start = int(((i - 1) / total_databases) * 100)
                set_progress(progress_start)
                print(f"Starting backup of database: {db_name} ({i}/{total_databases})")

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

                    # Update progress - completed this database
                    progress_complete = int((i / total_databases) * 100)
                    set_progress(progress_complete)

                else:
                    error_info = {
                        'database': db_name,
                        'error': result.stderr,
                        'status': 'failed'
                    }
                    backup_results['failed_backups'].append(error_info)
                    print(f" Failed to backup {db_name}: {result.stderr}")

                    # Update progress - completed this database (failed)
                    progress_complete = int((i / total_databases) * 100)
                    set_progress(progress_complete)

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

                # Update progress - completed this database (exception)
                progress_complete = int((i / total_databases) * 100)
                set_progress(progress_complete)

        # Summary
        successful_count = len(backup_results['successful_backups'])
        failed_count = len(backup_results['failed_backups'])

        print(f"\n=== Backup Summary ===")
        print(f"Total databases: {backup_results['total_databases']}")
        print(f"Successful backups: {successful_count}")
        print(f"Failed backups: {failed_count}")
        print(f"Base backup directory: {backup_directory}")
        print(f"Timestamped backup directory: {timestamped_backup_dir}")
        print(f"Directory timestamp: {dir_timestamp}")

        if successful_count > 0:
            total_size = sum(backup['file_size_mb'] for backup in backup_results['successful_backups'])
            print(f"Total backup size: {total_size:.2f} MB")

        # Final progress update - all backups completed
        set_progress(100)

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