import subprocess
import sys

def run(cmd):
    print(f"▶ Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        sys.exit(f"❌ Failed: {cmd}")

def build_bronze():
    run("python3 src/load_bronze.py")

def build_silver():
    run("sudo -u postgres psql -d postgres -f sql/silver_load.sql")

def build_gold():
    run("sudo -u postgres psql -d postgres -f sql/gold_tables.sql")

def all():
    build_bronze()
    build_silver()
    build_gold()
    print("✅ End-to-end ETL pipeline completed successfully")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 etl.py [bronze|silver|gold|all]")
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "bronze":
        build_bronze()
    elif cmd == "silver":
        build_silver()
    elif cmd == "gold":
        build_gold()
    elif cmd == "all":
        all()
    else:
        print("❌ Invalid command. Use: bronze | silver | gold | all")
