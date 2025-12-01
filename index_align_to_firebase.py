#!/usr/bin/env python3
"""
INDEX ALIGN TO FIREBASE ISSUES TRANSFER
Professional data pipeline for transferring issues data from Index Align database to Firebase
"""

import os
import pymysql
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from firebase_admin import credentials, initialize_app, db
import firebase_admin
from dotenv import load_dotenv
from datetime import datetime
import json
from typing import Dict, Any, Optional

# Load environment variables
load_dotenv()

def setup_firebase_realtime():
    """Connect to Firebase Realtime Database"""
    try:
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
        
        # Get reference to Realtime Database
        ref = db.reference()
        print("SUCCESS: Firebase Realtime Database connection ready")
        return ref
        
    except Exception as e:
        print(f"ERROR: Firebase Realtime Database connection failed: {str(e)}")
        return None

def connect_to_index_align_db():
    """Connect to Index Align database via SSH tunnel"""
    try:
        # SSH tunnel configuration
        ssh_host = os.getenv('INDEX_ALIGN_SSH_HOST')
        ssh_user = os.getenv('INDEX_ALIGN_SSH_USER')
        ssh_port = int(os.getenv('INDEX_ALIGN_SSH_PORT', '22'))
        
        db_host = os.getenv('INDEX_ALIGN_DB_HOST')
        db_port = int(os.getenv('INDEX_ALIGN_DB_PORT', '3306'))
        db_name = os.getenv('INDEX_ALIGN_DB_NAME')
        db_user = os.getenv('INDEX_ALIGN_DB_USER')
        db_password = os.getenv('INDEX_ALIGN_DB_PASSWORD')
        
        print(f"Setting up SSH tunnel to {ssh_host}...")
        
        # Create SSH tunnel
        ssh_key_path = os.getenv('INDEX_ALIGN_SSH_KEY_PATH')
        
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
            # Use password authentication
            tunnel = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_password=os.getenv('INDEX_ALIGN_SSH_PASSWORD'),
                remote_bind_address=(db_host, db_port),
                local_bind_address=('127.0.0.1', 0)
            )
        
        tunnel.start()
        print(f"SUCCESS: SSH tunnel established (local port: {tunnel.local_bind_port})")
        
        # Connect to MySQL through tunnel
        conn = pymysql.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=db_user,
            password=db_password,
            database=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("SUCCESS: Index Align database connection established")
        
        return conn, tunnel
        
    except Exception as e:
        print(f"ERROR: Index Align database connection failed: {str(e)}")
        print(f"Make sure SSH credentials are correct and SSH key is set up")
        return None, None

def get_issues_table_structure(conn):
    """Get the structure of the issues table"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM issues")
            columns = cursor.fetchall()
            print(f"ISSUES TABLE STRUCTURE:")
            print("=" * 50)
            for col in columns:
                print(f"  {col['Field']} ({col['Type']})")
            return [col['Field'] for col in columns]
    except Exception as e:
        print(f"ERROR: Failed to get table structure: {str(e)}")
        return None

def get_issues_from_database(conn):
    """Retrieve issues data from Index Align database"""
    try:
        # First, get table structure
        columns = get_issues_table_structure(conn)
        if not columns:
            return None
        
        # Query all issues
        query = "SELECT * FROM issues"
        
        df = pd.read_sql(query, conn)
        print(f"SUCCESS: Retrieved {len(df)} issues from Index Align database")
        
        # Display sample data
        if len(df) > 0:
            print("\nSample issues data:")
            print(df.head().to_string())
        
        return df
        
    except Exception as e:
        print(f"ERROR: Failed to retrieve issues: {str(e)}")
        return None

def get_company_id_from_ticker(ref, ticker):
    """Get company_id from ticker using the /tickers mapping in Firebase"""
    try:
        ticker_ref = ref.child('tickers').child(ticker)
        company_id = ticker_ref.get()
        if company_id:
            return company_id
        else:
            return None
    except Exception as e:
        print(f"  ERROR: Failed to lookup company_id for ticker {ticker}: {str(e)}")
        return None

def transform_issues_data(df, ref):
    """Transform issues data for Firebase structure: /issues/[company_id]/[issue_name]/Against, Neutral, Pro"""
    if df is None or df.empty:
        return None
    
    print("\nTRANSFORMING ISSUES DATA FOR FIREBASE")
    print("=" * 50)
    
    try:
        # Make a copy to avoid SettingWithCopyWarning
        df_transformed = df.copy()
        
        # Identify required columns
        # Look for ticker column (could be ticker, TICKER, company_ticker, etc.)
        ticker_column = None
        for possible_ticker in ['ticker', 'TICKER', 'company_ticker', 'COMPANY_TICKER', 'symbol', 'SYMBOL']:
            if possible_ticker in df_transformed.columns:
                ticker_column = possible_ticker
                break
        
        if ticker_column is None:
            print("ERROR: No ticker column found in issues table")
            print(f"Available columns: {list(df_transformed.columns)}")
            return None
        
        # Look for issue name column
        issue_name_column = None
        for possible_name in ['issue_name', 'ISSUE_NAME', 'issue', 'ISSUE', 'name', 'NAME']:
            if possible_name in df_transformed.columns:
                issue_name_column = possible_name
                break
        
        if issue_name_column is None:
            print("ERROR: No issue name column found in issues table")
            print(f"Available columns: {list(df_transformed.columns)}")
            return None
        
        # Look for Against, Neutral, Pro columns (case insensitive)
        against_column = None
        neutral_column = None
        pro_column = None
        
        for col in df_transformed.columns:
            col_lower = str(col).lower()
            if col_lower in ['against', 'against_amount', 'against_value']:
                against_column = col
            elif col_lower in ['neutral', 'neutral_amount', 'neutral_value']:
                neutral_column = col
            elif col_lower in ['pro', 'pro_amount', 'pro_value', 'for', 'for_amount']:
                pro_column = col
        
        if not against_column or not neutral_column or not pro_column:
            print("ERROR: Missing required columns (Against, Neutral, Pro)")
            print(f"Available columns: {list(df_transformed.columns)}")
            return None
        
        print(f"Using columns:")
        print(f"  Ticker: {ticker_column}")
        print(f"  Issue Name: {issue_name_column}")
        print(f"  Against: {against_column}")
        print(f"  Neutral: {neutral_column}")
        print(f"  Pro: {pro_column}")
        
        # Convert numeric columns to float
        for col in [against_column, neutral_column, pro_column]:
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce').fillna(0.0).astype(float)
        
        # Replace NaN with None for JSON serialization
        df_transformed = df_transformed.where(pd.notnull(df_transformed), None)
        
        # Build nested structure: issues[company_id][issue_name] = {Against: float, Neutral: float, Pro: float}
        issues_dict = {}
        ticker_to_company_id = {}
        skipped_tickers = set()
        
        print("\nMapping tickers to company_ids...")
        for _, row in df_transformed.iterrows():
            ticker = str(row[ticker_column]).strip().upper()
            
            # Skip if ticker is missing or already failed
            if pd.isna(row[ticker_column]) or ticker == 'NAN' or ticker == '':
                continue
            
            if ticker in skipped_tickers:
                continue
            
            # Get company_id from Firebase ticker mapping
            if ticker not in ticker_to_company_id:
                company_id = get_company_id_from_ticker(ref, ticker)
                if company_id:
                    ticker_to_company_id[ticker] = str(company_id)
                else:
                    print(f"  WARNING: No company_id mapping found for ticker {ticker}, skipping...")
                    skipped_tickers.add(ticker)
                    continue
            
            company_id = ticker_to_company_id[ticker]
            issue_name = str(row[issue_name_column]).strip()
            
            # Skip if issue name is missing
            if pd.isna(row[issue_name_column]) or issue_name == '':
                continue
            
            # Initialize company_id if not exists
            if company_id not in issues_dict:
                issues_dict[company_id] = {}
            
            # Add issue data
            issues_dict[company_id][issue_name] = {
                'Against': float(row[against_column]) if pd.notna(row[against_column]) else 0.0,
                'Neutral': float(row[neutral_column]) if pd.notna(row[neutral_column]) else 0.0,
                'Pro': float(row[pro_column]) if pd.notna(row[pro_column]) else 0.0
            }
        
        print(f"\nSUCCESS: Transformed issues data")
        print(f"  Companies processed: {len(issues_dict)}")
        print(f"  Tickers skipped (no mapping): {len(skipped_tickers)}")
        
        # Validate exactly 8 issues per company
        companies_with_wrong_count = []
        for company_id, issues in issues_dict.items():
            if len(issues) != 8:
                companies_with_wrong_count.append((company_id, len(issues)))
        
        if companies_with_wrong_count:
            print(f"\nWARNING: {len(companies_with_wrong_count)} companies don't have exactly 8 issues:")
            for company_id, count in companies_with_wrong_count[:10]:  # Show first 10
                print(f"  Company {company_id}: {count} issues")
        else:
            print(f"  [OK] All companies have exactly 8 issues")
        
        # Show sample transformed data
        print("\nSample transformed data:")
        if issues_dict:
            sample_company_id = list(issues_dict.keys())[0]
            sample_issues = issues_dict[sample_company_id]
            sample_issue_name = list(sample_issues.keys())[0]
            print(f"  Company ID: {sample_company_id}")
            print(f"  Issue: {sample_issue_name}")
            print(json.dumps({sample_issue_name: sample_issues[sample_issue_name]}, indent=2))
        
        return issues_dict
        
    except Exception as e:
        print(f"ERROR: Data transformation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def export_data_to_json(issues_dict, output_dir="."):
    """Export all issues data to JSON file for manual review"""
    try:
        from datetime import datetime
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = os.path.join(output_dir, f"issues_review_{timestamp}.json")
        
        # Prepare data structure with metadata
        export_data = {
            "export_info": {
                "timestamp": datetime.now().isoformat(),
                "total_companies": len(issues_dict),
                "total_issues": sum(len(issues) for issues in issues_dict.values()),
                "firebase_path": "/issues/[company_id]/[issue_name]/Against, Neutral, Pro"
            },
            "companies": {}
        }
        
        # Add all companies with their issues
        for company_id, issues in issues_dict.items():
            company_data = {
                "company_id": company_id,
                "total_issues": len(issues),
                "issues": {}
            }
            
            # Add all issues for this company
            for issue_name, values in sorted(issues.items()):
                against = values.get('Against', 0.0)
                neutral = values.get('Neutral', 0.0)
                pro = values.get('Pro', 0.0)
                total = against + neutral + pro
                
                # Determine position (highest value)
                if total > 0:
                    if against >= neutral and against >= pro:
                        position = "AGAINST"
                    elif pro >= neutral and pro >= against:
                        position = "PRO"
                    else:
                        position = "NEUTRAL"
                else:
                    position = "NEUTRAL"
                
                company_data["issues"][issue_name] = {
                    "Against": float(against),
                    "Neutral": float(neutral),
                    "Pro": float(pro),
                    "Total": float(total),
                    "Position": position
                }
            
            export_data["companies"][company_id] = company_data
        
        # Write to JSON file with pretty formatting
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        return json_filename, export_data
        
    except Exception as e:
        print(f"ERROR: Failed to export JSON file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

def display_data_visualization(issues_dict, df_original=None):
    """Display summary and export all data to JSON file for manual review"""
    print("\n" + "=" * 80)
    print("DATA VISUALIZATION - MANUAL REVIEW REQUIRED")
    print("=" * 80)
    
    if not issues_dict or len(issues_dict) == 0:
        print("ERROR: No data to visualize")
        return False
    
    # Summary Statistics
    print("\nSUMMARY STATISTICS")
    print("-" * 80)
    total_companies = len(issues_dict)
    total_issues = sum(len(issues) for issues in issues_dict.values())
    avg_issues_per_company = total_issues / total_companies if total_companies > 0 else 0
    
    print(f"  Total Companies: {total_companies}")
    print(f"  Total Issues: {total_issues}")
    print(f"  Average Issues per Company: {avg_issues_per_company:.2f}")
    
    # Check for companies with exactly 8 issues
    companies_with_8_issues = sum(1 for issues in issues_dict.values() if len(issues) == 8)
    companies_without_8_issues = total_companies - companies_with_8_issues
    
    print(f"  Companies with exactly 8 issues: {companies_with_8_issues}")
    if companies_without_8_issues > 0:
        print(f"  [WARNING] Companies without 8 issues: {companies_without_8_issues}")
    
    # Export to JSON file
    print("\nEXPORTING ALL DATA TO JSON FILE")
    print("-" * 80)
    json_filename, export_data = export_data_to_json(issues_dict)
    
    if json_filename:
        file_path = os.path.abspath(json_filename)
        file_size = os.path.getsize(json_filename) / 1024  # Size in KB
        
        print(f"  [SUCCESS] Successfully exported all data to JSON file")
        print(f"  File location: {file_path}")
        print(f"  File size: {file_size:.2f} KB")
        print(f"  Contains: {total_companies} companies, {total_issues} total issues")
        print(f"\n  [IMPORTANT] Please review the JSON file before approving upload!")
        print(f"     Open the file in a text editor or JSON viewer to see all data.")
        
        # Show sample of what's in the file
        print(f"\n  Sample structure in JSON file:")
        if export_data and export_data.get("companies"):
            sample_company_id = list(export_data["companies"].keys())[0]
            sample_company = export_data["companies"][sample_company_id]
            sample_issue_name = list(sample_company["issues"].keys())[0] if sample_company["issues"] else None
            
            print(f"    {{")
            print(f"      \"export_info\": {{ ... }},\n      \"companies\": {{")
            print(f"        \"{sample_company_id}\": {{")
            print(f"          \"company_id\": \"{sample_company_id}\",")
            print(f"          \"total_issues\": {sample_company['total_issues']},")
            print(f"          \"issues\": {{")
            if sample_issue_name:
                sample_issue = sample_company["issues"][sample_issue_name]
                print(f"            \"{sample_issue_name}\": {{")
                print(f"              \"Against\": {sample_issue['Against']},")
                print(f"              \"Neutral\": {sample_issue['Neutral']},")
                print(f"              \"Pro\": {sample_issue['Pro']},")
                print(f"              \"Total\": {sample_issue['Total']},")
                print(f"              \"Position\": \"{sample_issue['Position']}\"")
                print(f"            }}, ...")
            print(f"          }}")
            print(f"        }}, ...")
            print(f"      }}")
            print(f"    }}")
        
        return json_filename
    else:
        print("  [ERROR] Failed to export JSON file")
        return None

def upload_issues_to_firebase(ref, issues_dict, dry_run=False):
    """Upload issues to Firebase Realtime Database under /issues/[company_id]/[issue_name] path"""
    if dry_run:
        print("\nDRY RUN MODE - Testing issues upload")
        print("=" * 50)
        print(f"Would upload issues for {len(issues_dict)} companies")
        
        # Count total issues
        total_issues = sum(len(issues) for issues in issues_dict.values())
        print(f"Total issues: {total_issues}")
        
        print("\nSample company data:")
        for i, (company_id, issues) in enumerate(list(issues_dict.items())[:3]):
            print(f"\nCompany {i+1} (ID: {company_id}): {len(issues)} issues")
            for issue_name, values in list(issues.items())[:2]:
                print(f"  {issue_name}:")
                print(f"    Against: {values['Against']}")
                print(f"    Neutral: {values['Neutral']}")
                print(f"    Pro: {values['Pro']}")
        print("\nWould upload to Firebase path: /issues/[company_id]/[issue_name]")
        print("Note: This will overwrite all existing data for each company_id")
        return True
    else:
        print("\nUPLOADING ISSUES TO FIREBASE")
        print("=" * 50)
        
        try:
            # Upload to /issues path
            issues_ref = ref.child('issues')
            
            success_count = 0
            skipped_count = 0
            
            # Upload each company's issues (overwrites entire company object)
            for company_id, company_issues in issues_dict.items():
                try:
                    # Upload entire company object - this overwrites everything for that company_id
                    company_ref = issues_ref.child(str(company_id))
                    company_ref.set(company_issues)
                    
                    success_count += 1
                    issue_count = len(company_issues)
                    print(f"  [SUCCESS] Uploaded company {company_id}: {issue_count} issues")
                    
                except Exception as e:
                    print(f"  [ERROR] Failed to upload company {company_id}: {str(e)}")
                    skipped_count += 1
                    continue
            
            print(f"\nSUCCESS: Uploaded issues for {success_count} companies")
            if skipped_count > 0:
                print(f"SKIPPED: {skipped_count} companies failed to upload")
            
            # Verify upload
            uploaded_companies = len(issues_ref.get() or {})
            print(f"VERIFIED: {uploaded_companies} companies now in Firebase /issues")
            
            return success_count > 0
            
        except Exception as e:
            print(f"ERROR: Failed to upload issues: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Main function to run the Index Align to Firebase issues pipeline"""
    print("INDEX ALIGN TO FIREBASE ISSUES TRANSFER")
    print("=" * 60)
    
    tunnel = None
    conn = None
    
    try:
        # Step 1: Setup Firebase connection
        print("\nStep 1: Setting up Firebase connection...")
        firebase_ref = setup_firebase_realtime()
        if not firebase_ref:
            return False
        
        # Step 2: Connect to Index Align database
        print("\nStep 2: Connecting to Index Align database...")
        conn, tunnel = connect_to_index_align_db()
        if not conn or not tunnel:
            return False
        
        # Step 3: Get issues data
        print("\nStep 3: Retrieving issues from Index Align database...")
        df = get_issues_from_database(conn)
        if df is None or df.empty:
            print("ERROR: No issues retrieved from database")
            return False
        
        # Step 4: Transform data
        print("\nStep 4: Transforming issues data...")
        issues_dict = transform_issues_data(df, firebase_ref)
        if issues_dict is None:
            print("ERROR: Data transformation failed")
            return False
        
        # Step 5: Display data visualization and export to JSON for manual review
        print("\nStep 5: Exporting all data to JSON file for manual review...")
        json_filename = display_data_visualization(issues_dict, df)
        if not json_filename:
            print("ERROR: Failed to export data for review")
            return False
        
        # Step 6: Manual approval required before upload
        print("\n" + "=" * 80)
        print("MANUAL REVIEW REQUIRED")
        print("=" * 80)
        json_file_path = os.path.abspath(json_filename)
        print(f"\nAll data has been exported to:")
        print(f"  File: {json_file_path}")
        
        # Try to open the file automatically (platform-specific)
        try:
            import platform
            system = platform.system()
            if system == 'Darwin':  # macOS
                os.system(f'open "{json_file_path}"')
                print(f"  [OK] Opened JSON file in default application")
            elif system == 'Windows':
                os.system(f'start "" "{json_file_path}"')
                print(f"  [OK] Opened JSON file in default application")
            elif system == 'Linux':
                os.system(f'xdg-open "{json_file_path}"')
                print(f"  [OK] Opened JSON file in default application")
        except Exception as e:
            print(f"  (Could not auto-open file: {str(e)})")
        
        print("\nPLEASE REVIEW THE JSON FILE BEFORE PROCEEDING:")
        print("   1. Review all companies and their issues data in the JSON file")
        print("   2. Verify the data is correct")
        print("   3. Check that all companies have exactly 8 issues")
        print("   4. Verify Against, Neutral, and Pro values are correct")
        print("   5. Return here and approve when ready")
        print("\nThis data will be uploaded to Firebase Realtime Database at:")
        print("  Path: /issues/[company_id]/[issue_name]/Against, Neutral, Pro")
        print("\n[WARNING] This will OVERWRITE all existing issues data for each company_id")
        print("\n" + "=" * 80)
        
        # Ask for approval with multiple confirmation options
        approved = False
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                user_input = input("\nDo you APPROVE this data for upload to Firebase? (yes/no): ").lower().strip()
                
                if user_input in ['yes', 'y', 'approve', 'ok']:
                    approved = True
                    break
                elif user_input in ['no', 'n', 'cancel', 'abort']:
                    print("\n[CANCELLED] Upload cancelled by user")
                    print("No data was uploaded to Firebase.")
                    return False
                else:
                    remaining = max_attempts - attempt - 1
                    if remaining > 0:
                        print(f"\n[WARNING] Invalid input. Please type 'yes' to approve or 'no' to cancel.")
                        print(f"   ({remaining} attempts remaining)")
                    else:
                        print("\n[CANCELLED] Maximum attempts reached. Upload cancelled for safety.")
                        return False
            except EOFError:
                print("\n[CANCELLED] Input interrupted. Upload cancelled for safety.")
                return False
            except KeyboardInterrupt:
                print("\n\n[CANCELLED] Upload cancelled by user (Ctrl+C)")
                return False
        
        if not approved:
            print("\n[CANCELLED] Upload not approved. Exiting without uploading.")
            return False
        
        # Step 7: Upload to Firebase (only after approval)
        print("\nStep 7: Uploading approved data to Firebase...")
        print("=" * 80)
        success = upload_issues_to_firebase(firebase_ref, issues_dict, dry_run=False)
        
        if success:
            print("\nSUCCESS: Index Align to Firebase issues pipeline completed successfully")
            return True
        else:
            print("\nERROR: Issues pipeline failed")
            return False
        
    except Exception as e:
        print(f"\nERROR: Pipeline failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Step 8: Cleanup connections
        print("\nStep 8: Closing connections...")
        if conn:
            conn.close()
            print("SUCCESS: Database connection closed")
        if tunnel:
            tunnel.stop()
            print("SUCCESS: SSH tunnel closed")

if __name__ == "__main__":
    main()

