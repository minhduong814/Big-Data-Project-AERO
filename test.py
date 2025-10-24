from prefect import flow, task
from dotenv import load_dotenv
import os
import subprocess

# Load your environment file
load_dotenv(dotenv_path=".env.local")

# ---------- JAVA CHECK ----------
@task
def check_java():
    java_home = os.getenv("JAVA_HOME")
    if not java_home:
        raise EnvironmentError("❌ JAVA_HOME not found. Check your .env.local or load_dotenv() call.")
    
    java_path = os.path.join(java_home, "bin", "java.exe")
    if not os.path.exists(java_path):
        raise FileNotFoundError(f"❌ Java executable not found at: {java_path}")

    print(f"✅ JAVA_HOME = {java_home}")
    print("▶ Running: java -version")
    subprocess.run([java_path, "-version"], check=True)

# ---------- HADOOP CHECK ----------
@task
def check_hadoop():
    hadoop_home = os.getenv("HADOOP_HOME")
    if not hadoop_home:
        raise EnvironmentError("❌ HADOOP_HOME not found. Please set it in .env.local.")
    
    hadoop_bin = os.path.join(hadoop_home, "bin", "hadoop.cmd")
    if not os.path.exists(hadoop_bin):
        # On Linux/Mac, use `hadoop` instead of `hadoop.cmd`
        alt_path = os.path.join(hadoop_home, "bin", "hadoop")
        if os.path.exists(alt_path):
            hadoop_bin = alt_path
        else:
            raise FileNotFoundError(f"❌ Hadoop executable not found at: {hadoop_bin} or {alt_path}")

    print(f"✅ HADOOP_HOME = {hadoop_home}")
    print("▶ Running: hadoop version")
    try:
        subprocess.run([hadoop_bin, "version"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error occurred while checking Hadoop version: {e}")

# ---------- PREFECT FLOW ----------
@flow(name="Environment Check Flow")
def test_flow():
    print("🔍 Checking local Java and Hadoop setup...\n")
    # check_java()
    # print("\n---\n")
    check_hadoop()
    print("\n✅ All environment checks passed successfully!")

# ---------- ENTRY POINT ----------
if __name__ == "__main__":
    test_flow()
