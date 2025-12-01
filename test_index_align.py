#!/usr/bin/env python3
"""
Test script for Index Align to Firebase connection
Run this before running the main pipeline
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_environment_variables():
    """Test if all required environment variables are set"""
    print("TESTING ENVIRONMENT VARIABLES")
    print("=" * 50)
    
    required_vars = {
        'Firebase': [
            'FIREBASE_PROJECT_ID',
            'FIREBASE_PRIVATE_KEY_ID',
            'FIREBASE_PRIVATE_KEY',
            'FIREBASE_CLIENT_EMAIL',
            'FIREBASE_CLIENT_ID'
        ],
        'Index Align': [
            'INDEX_ALIGN_SSH_HOST',
            'INDEX_ALIGN_SSH_USER',
            'INDEX_ALIGN_DB_NAME',
            'INDEX_ALIGN_DB_USER',
            'INDEX_ALIGN_DB_PASSWORD'
        ]
    }
    
    missing_vars = []
    all_present = True
    
    for category, vars_list in required_vars.items():
        print(f"\n{category} Variables:")
        for var in vars_list:
            value = os.getenv(var)
            if value and value.strip() and 'your_' not in value.lower():
                print(f"  [OK] {var}: {'*' * 20} (set)")
            else:
                print(f"  [ERROR] {var}: NOT SET")
                missing_vars.append(var)
                all_present = False
    
    # Check SSH authentication method
    print("\nSSH Authentication:")
    ssh_key_path = os.getenv('INDEX_ALIGN_SSH_KEY_PATH')
    ssh_password = os.getenv('INDEX_ALIGN_SSH_PASSWORD')
    
    if ssh_key_path and os.path.exists(ssh_key_path):
        print(f"  [OK] Using SSH key: {ssh_key_path}")
    elif ssh_password:
        print(f"  [OK] Using SSH password")
    else:
        print(f"  [ERROR] No SSH authentication method set")
        print(f"    Set either INDEX_ALIGN_SSH_KEY_PATH or INDEX_ALIGN_SSH_PASSWORD")
        all_present = False
    
    if all_present:
        print("\n[SUCCESS] All required environment variables are set")
    else:
        print(f"\n[ERROR] Missing {len(missing_vars)} environment variables")
        print("Please update your .env file")
    
    return all_present

def test_ssh_connection():
    """Test SSH connection to Index Align"""
    print("\n\nTESTING SSH CONNECTION")
    print("=" * 50)
    
    try:
        import paramiko
        from sshtunnel import SSHTunnelForwarder
        
        ssh_host = os.getenv('INDEX_ALIGN_SSH_HOST')
        ssh_user = os.getenv('INDEX_ALIGN_SSH_USER')
        ssh_port = int(os.getenv('INDEX_ALIGN_SSH_PORT', '22'))
        
        print(f"Connecting to {ssh_host}...")
        
        ssh_key_path = os.getenv('INDEX_ALIGN_SSH_KEY_PATH')
        ssh_password = os.getenv('INDEX_ALIGN_SSH_PASSWORD')
        
        # Test SSH connection
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        if ssh_key_path and os.path.exists(ssh_key_path):
            ssh.connect(ssh_host, username=ssh_user, port=ssh_port, key_filename=ssh_key_path)
        elif ssh_password:
            ssh.connect(ssh_host, username=ssh_user, port=ssh_port, password=ssh_password)
        else:
            print("[ERROR] No SSH authentication configured")
            return False
        
        stdin, stdout, stderr = ssh.exec_command('whoami')
        user = stdout.read().decode().strip()
        print(f"[OK] Successfully connected as: {user}")
        
        stdin, stdout, stderr = ssh.exec_command('pwd')
        pwd = stdout.read().decode().strip()
        print(f"  Current directory: {pwd}")
        
        ssh.close()
        print("[SUCCESS] SSH connection test passed")
        return True
        
    except ImportError:
        print("[ERROR] paramiko or sshtunnel not installed")
        print("  Run: pip install paramiko sshtunnel")
        return False
    except Exception as e:
        print(f"[ERROR] SSH connection failed: {str(e)}")
        return False

def test_database_connection():
    """Test database connection through SSH tunnel"""
    print("\n\nTESTING DATABASE CONNECTION")
    print("=" * 50)
    
    try:
        import pymysql
        from sshtunnel import SSHTunnelForwarder
        
        ssh_host = os.getenv('INDEX_ALIGN_SSH_HOST')
        ssh_user = os.getenv('INDEX_ALIGN_SSH_USER')
        ssh_port = int(os.getenv('INDEX_ALIGN_SSH_PORT', '22'))
        db_host = os.getenv('INDEX_ALIGN_DB_HOST')
        db_port = int(os.getenv('INDEX_ALIGN_DB_PORT', '3306'))
        db_name = os.getenv('INDEX_ALIGN_DB_NAME')
        db_user = os.getenv('INDEX_ALIGN_DB_USER')
        db_password = os.getenv('INDEX_ALIGN_DB_PASSWORD')
        
        print(f"Setting up SSH tunnel to {ssh_host}...")
        
        ssh_key_path = os.getenv('INDEX_ALIGN_SSH_KEY_PATH')
        ssh_password = os.getenv('INDEX_ALIGN_SSH_PASSWORD')
        
        # Create SSH tunnel
        if ssh_key_path and os.path.exists(ssh_key_path):
            # Read SSH key using context manager to ensure file is properly closed
            with open(ssh_key_path, 'r') as key_file:
                ssh_key = key_file.read()
            tunnel = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_pkey=ssh_key,
                remote_bind_address=(db_host, db_port),
                local_bind_address=('127.0.0.1', 0)
            )
        else:
            tunnel = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_password=ssh_password,
                remote_bind_address=(db_host, db_port),
                local_bind_address=('127.0.0.1', 0)
            )
        
        tunnel.start()
        print(f"[OK] SSH tunnel established (local port: {tunnel.local_bind_port})")
        
        # Connect to MySQL
        conn = pymysql.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=db_user,
            password=db_password,
            database=db_name,
            charset='utf8mb4'
        )
        print(f"[OK] Connected to database: {db_name}")
        
        # Test query
        cursor = conn.cursor()
        
        # Check if issues table exists
        cursor.execute("SHOW TABLES LIKE 'issues'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            print("[OK] Issues table exists")
            
            # Get row count
            cursor.execute("SELECT COUNT(*) FROM issues")
            count = cursor.fetchone()[0]
            print(f"  Issues count: {count}")
            
            # Get table structure
            cursor.execute("SHOW COLUMNS FROM issues")
            columns = cursor.fetchall()
            print(f"  Columns: {len(columns)}")
            for col in columns[:5]:  # Show first 5 columns
                print(f"    - {col[0]} ({col[1]})")
        
        cursor.close()
        conn.close()
        tunnel.stop()
        
        print("[SUCCESS] Database connection test passed")
        return True
        
    except ImportError:
        print("[ERROR] pymysql not installed")
        print("  Run: pip install pymysql")
        return False
    except Exception as e:
        print(f"[ERROR] Database connection failed: {str(e)}")
        return False

def test_firebase_connection():
    """Test Firebase connection"""
    print("\n\nTESTING FIREBASE CONNECTION")
    print("=" * 50)
    
    try:
        import firebase_admin
        from firebase_admin import credentials, initialize_app, db
        
        if not firebase_admin._apps:
            private_key = os.getenv('FIREBASE_PRIVATE_KEY')
            if not private_key:
                raise ValueError("FIREBASE_PRIVATE_KEY environment variable is not set")
            
            cred_info = {
                "type": "service_account",
                "project_id": os.getenv('FIREBASE_PROJECT_ID'),
                "private_key_id": os.getenv('FIREBASE_PRIVATE_KEY_ID'),
                "private_key": private_key.replace('\\n', '\n'),
                "client_email": os.getenv('FIREBASE_CLIENT_EMAIL'),
                "client_id": os.getenv('FIREBASE_CLIENT_ID'),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://accounts.google.com/o/oauth2/token"
            }
            cred = credentials.Certificate(cred_info)
            initialize_app(cred, {
                'databaseURL': f"https://{os.getenv('FIREBASE_PROJECT_ID')}-default-rtdb.firebaseio.com/"
            })
        
        ref = db.reference()
        print("[OK] Connected to Firebase Realtime Database")
        
        # Test read
        test_path = ref.child('test_connection')
        test_path.set({'timestamp': str(os.path.getmtime(__file__))})
        data = test_path.get()
        print(f"[OK] Read/Write test successful")
        test_path.delete()
        
        print("[SUCCESS] Firebase connection test passed")
        return True
        
    except ImportError:
        print("[ERROR] firebase-admin not installed")
        print("  Run: pip install firebase-admin")
        return False
    except Exception as e:
        print(f"[ERROR] Firebase connection failed: {str(e)}")
        return False

def main():
    """Run all tests"""
    print("INDEX ALIGN TO FIREBASE - CONNECTION TEST")
    print("=" * 60)
    
    results = {}
    
    # Test 1: Environment variables
    results['env'] = test_environment_variables()
    
    if not results['env']:
        print("\nPlease fix environment variables before proceeding")
        return
    
    # Test 2: SSH connection
    results['ssh'] = test_ssh_connection()
    
    # Test 3: Database connection
    results['db'] = test_database_connection()
    
    # Test 4: Firebase connection
    results['firebase'] = test_firebase_connection()
    
    # Summary
    print("\n\nTEST SUMMARY")
    print("=" * 50)
    for test_name, passed in results.items():
        status = "[PASS]" if passed else "[FAIL]"
        print(f"{test_name.upper()}: {status}")
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\n[SUCCESS] All tests passed! You're ready to run the pipeline.")
        print("Run: python index_align_to_firebase.py")
    else:
        print(f"\n[WARNING] {sum(not v for v in results.values())} test(s) failed")
        print("Please fix the issues before running the pipeline")

if __name__ == "__main__":
    main()

