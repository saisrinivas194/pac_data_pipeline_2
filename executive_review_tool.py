#!/usr/bin/env python3
"""
EXECUTIVE RECORD REVIEW TOOL
Groups executive records that look like the same person (across all companies).
Since executives can be at multiple companies (e.g., Elon Musk at Tesla, SpaceX, Twitter),
this tool groups by person first, then tracks all companies they're associated with.
Contributions from that person should count towards all companies they're an executive of.
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
from typing import Dict, Any, Optional, List, Tuple
from rapidfuzz import fuzz, process
from collections import defaultdict

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
        
        ref = db.reference()
        print("SUCCESS: Firebase Realtime Database connection ready")
        return ref
        
    except Exception as e:
        print(f"ERROR: Firebase Realtime Database connection failed: {str(e)}")
        return None

def connect_to_index_align_db():
    """Connect to Index Align database via SSH tunnel"""
    try:
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
        
        if ssh_key_path and os.path.exists(ssh_key_path):
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
                ssh_password=os.getenv('INDEX_ALIGN_SSH_PASSWORD'),
                remote_bind_address=(db_host, db_port),
                local_bind_address=('127.0.0.1', 0)
            )
        
        tunnel.start()
        print(f"SUCCESS: SSH tunnel established (local port: {tunnel.local_bind_port})")
        
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
        return None, None

def get_executive_table_structure(conn):
    """Get the structure of the executives table"""
    try:
        with conn.cursor() as cursor:
            # Try common table names
            for table_name in ['executives', 'executive', 'execs', 'exec']:
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if cursor.fetchone():
                    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
                    columns = cursor.fetchall()
                    print(f"EXECUTIVES TABLE STRUCTURE ({table_name}):")
                    print("=" * 50)
                    for col in columns:
                        print(f"  {col['Field']} ({col['Type']})")
                    return table_name, [col['Field'] for col in columns]
            
            # If no executives table found, show all tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print("Available tables:")
            for table in tables:
                print(f"  - {list(table.values())[0]}")
            return None, None
    except Exception as e:
        print(f"ERROR: Failed to get table structure: {str(e)}")
        return None, None

def get_executives_from_database(conn, table_name='executives'):
    """Retrieve executive records from Index Align database"""
    try:
        # First, get table structure
        table_name, columns = get_executive_table_structure(conn)
        if not table_name or not columns:
            print("ERROR: Could not find executives table")
            return None
        
        # Query all executives
        query = f"SELECT * FROM {table_name}"
        
        df = pd.read_sql(query, conn)
        print(f"SUCCESS: Retrieved {len(df)} executive records from Index Align database")
        
        # Display sample data
        if len(df) > 0:
            print("\nSample executive data:")
            print(df.head().to_string())
        
        return df
        
    except Exception as e:
        print(f"ERROR: Failed to retrieve executives: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def normalize_string(s):
    """Normalize string for comparison"""
    if pd.isna(s) or s is None:
        return ""
    s = str(s).strip().lower()
    # Remove common punctuation and extra spaces
    s = ' '.join(s.split())
    return s

def extract_name_parts(name):
    """Extract first and last name parts"""
    if not name or pd.isna(name):
        return "", ""
    name = str(name).strip()
    parts = name.split()
    if len(parts) >= 2:
        return parts[0].lower(), parts[-1].lower()
    elif len(parts) == 1:
        return parts[0].lower(), ""
    return "", ""

def calculate_similarity_score(record1: Dict, record2: Dict, name_col: str, 
                                title_col: str, address_col: str, company_col: str) -> float:
    """
    Calculate similarity score between two executive records.
    Since executives can be at multiple companies, we prioritize name matching
    and don't require company match (person can be at different companies).
    """
    scores = []
    weights = []
    
    # Name similarity (weight: 0.5) - Most important since we're matching across companies
    name1 = normalize_string(record1.get(name_col, ""))
    name2 = normalize_string(record2.get(name_col, ""))
    if name1 and name2:
        # Use token sort ratio for better name matching (handles "John Smith" vs "Smith, John")
        name_score = fuzz.token_sort_ratio(name1, name2)
        scores.append(name_score)
        weights.append(0.5)
    
    # Address similarity (weight: 0.25) - Strong indicator of same person
    address1 = normalize_string(record1.get(address_col, ""))
    address2 = normalize_string(record2.get(address_col, ""))
    if address1 and address2:
        address_score = fuzz.token_sort_ratio(address1, address2)
        scores.append(address_score)
        weights.append(0.25)
    
    # Title similarity (weight: 0.15) - Can help but less important
    title1 = normalize_string(record1.get(title_col, ""))
    title2 = normalize_string(record2.get(title_col, ""))
    if title1 and title2:
        title_score = fuzz.token_sort_ratio(title1, title2)
        scores.append(title_score)
        weights.append(0.15)
    
    # Company name similarity (weight: 0.1) - Least important since person can be at multiple companies
    # But if companies match, it's a bonus signal
    company1 = normalize_string(record1.get(company_col, ""))
    company2 = normalize_string(record2.get(company_col, ""))
    if company1 and company2:
        company_score = fuzz.ratio(company1, company2)
        scores.append(company_score)
        weights.append(0.1)
    
    # Calculate weighted average
    if not scores:
        return 0.0
    
    total_weight = sum(weights)
    if total_weight == 0:
        return 0.0
    
    weighted_score = sum(s * w for s, w in zip(scores, weights)) / total_weight
    return weighted_score

def identify_column_names(df):
    """Identify column names for name, title, address, company"""
    name_col = None
    title_col = None
    address_col = None
    company_col = None
    
    # Common variations of column names
    name_variations = ['name', 'executive_name', 'person_name', 'full_name', 
                       'first_name', 'last_name', 'exec_name']
    title_variations = ['title', 'job_title', 'position', 'role', 'job', 'exec_title']
    address_variations = ['address', 'location', 'city', 'state', 'address_line', 
                         'street', 'mailing_address']
    company_variations = ['company', 'company_name', 'employer', 'firm', 
                         'organization', 'org', 'company_name']
    
    for col in df.columns:
        col_lower = str(col).lower()
        
        if not name_col and any(var in col_lower for var in name_variations):
            name_col = col
        if not title_col and any(var in col_lower for var in title_variations):
            title_col = col
        if not address_col and any(var in col_lower for var in address_variations):
            address_col = col
        if not company_col and any(var in col_lower for var in company_variations):
            company_col = col
    
    return name_col, title_col, address_col, company_col

def group_executive_records(df, name_col: str, title_col: str, 
                           address_col: str, company_col: str,
                           similarity_threshold: float = 75.0,
                           uncertainty_threshold: float = 85.0) -> Tuple[List, List]:
    """
    Group executive records that look like the same PERSON (across all companies).
    Since executives can be at multiple companies, we group by person first,
    then track all companies they're associated with.
    
    Returns:
        - groups: List of group dictionaries with all records for each person
        - uncertain_groups: List of group_ids that need manual review
    """
    print("\nGROUPING EXECUTIVE RECORDS BY PERSON")
    print("=" * 50)
    print(f"Total records: {len(df)}")
    print(f"Similarity threshold (grouping): {similarity_threshold}%")
    print(f"Uncertainty threshold (review): {uncertainty_threshold}%")
    print("\nNOTE: Grouping by PERSON (not person+company)")
    print("      Executives at multiple companies will be grouped together")
    
    # Convert dataframe to list of dicts for easier processing
    records = df.to_dict('records')
    
    # Track which records have been grouped
    grouped = set()
    groups = []
    group_id = 0
    
    # For each ungrouped record, find similar records (same person, any company)
    for i, record1 in enumerate(records):
        if i in grouped:
            continue
        
        # Start a new group with this record
        current_group = [i]
        grouped.add(i)
        
        # Find similar records (same person, potentially different companies)
        for j, record2 in enumerate(records):
            if j <= i or j in grouped:
                continue
            
            similarity = calculate_similarity_score(
                record1, record2, name_col, title_col, address_col, company_col
            )
            
            # If similarity is above threshold, add to group (even if different companies)
            if similarity >= similarity_threshold:
                current_group.append(j)
                grouped.add(j)
        
        # Only create group if it has multiple records
        if len(current_group) > 1:
            # Get all unique companies for this person
            companies = set()
            for idx in current_group:
                company = normalize_string(records[idx].get(company_col, ""))
                if company:
                    companies.add(company)
            
            groups.append({
                'group_id': group_id,
                'record_indices': current_group,
                'records': [records[idx] for idx in current_group],
                'companies': list(companies),
                'person_name': normalize_string(records[current_group[0]].get(name_col, ""))
            })
            group_id += 1
    
    print(f"Found {len(groups)} groups with multiple records")
    print(f"  (These represent {sum(len(g['records']) for g in groups)} records grouped by person)")
    
    # Identify uncertain groups (those that need review)
    uncertain_groups = []
    for group in groups:
        if len(group['records']) < 2:
            continue
        
        # Calculate average similarity within group
        similarities = []
        records_in_group = group['records']
        
        for i in range(len(records_in_group)):
            for j in range(i + 1, len(records_in_group)):
                sim = calculate_similarity_score(
                    records_in_group[i], records_in_group[j],
                    name_col, title_col, address_col, company_col
                )
                similarities.append(sim)
        
        if similarities:
            avg_similarity = sum(similarities) / len(similarities)
            # If average similarity is between thresholds, it's uncertain
            if similarity_threshold <= avg_similarity < uncertainty_threshold:
                uncertain_groups.append(group['group_id'])
                group['avg_similarity'] = avg_similarity
                group['confidence'] = 'uncertain'
            elif avg_similarity >= uncertainty_threshold:
                group['avg_similarity'] = avg_similarity
                group['confidence'] = 'high'
            else:
                group['avg_similarity'] = avg_similarity
                group['confidence'] = 'low'
    
    print(f"Uncertain groups requiring review: {len(uncertain_groups)}")
    print(f"High confidence groups (auto-approved): {len([g for g in groups if g.get('confidence') == 'high'])}")
    
    return groups, uncertain_groups

def export_review_data(groups: List[Dict], uncertain_groups: List[int], 
                       df: pd.DataFrame, name_col: str, title_col: str,
                       address_col: str, company_col: str, output_dir="."):
    """Export uncertain groups to JSON file for review"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = os.path.join(output_dir, f"executive_review_{timestamp}.json")
        
        # Only export uncertain groups
        review_groups = [g for g in groups if g['group_id'] in uncertain_groups]
        
        export_data = {
            "export_info": {
                "timestamp": datetime.now().isoformat(),
                "total_groups": len(review_groups),
                "total_records": sum(len(g['records']) for g in review_groups),
                "review_type": "uncertain_executive_matches"
            },
            "groups": []
        }
        
        for group in review_groups:
            group_data = {
                "group_id": group['group_id'],
                "confidence": group.get('confidence', 'uncertain'),
                "average_similarity": round(group.get('avg_similarity', 0), 2),
                "record_count": len(group['records']),
                "companies": group.get('companies', []),
                "person_name": group.get('person_name', ''),
                "records": []
            }
            
            for record in group['records']:
                record_data = {
                    "name": str(record.get(name_col, "")),
                    "title": str(record.get(title_col, "")),
                    "company": str(record.get(company_col, "")),
                    "address": str(record.get(address_col, "")),
                    "all_fields": {k: str(v) for k, v in record.items() if not pd.isna(v)}
                }
                group_data["records"].append(record_data)
            
            export_data["groups"].append(group_data)
        
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        return json_filename, export_data
        
    except Exception as e:
        print(f"ERROR: Failed to export review data: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

def display_review_interface(groups: List[Dict], uncertain_groups: List[int],
                             name_col: str, title_col: str, address_col: str, company_col: str):
    """Display review interface for uncertain groups"""
    print("\n" + "=" * 80)
    print("EXECUTIVE RECORD REVIEW - UNCERTAIN MATCHES")
    print("=" * 80)
    
    review_groups = [g for g in groups if g['group_id'] in uncertain_groups]
    
    if not review_groups:
        print("\n[SUCCESS] No uncertain matches found. All groups have high confidence.")
        return [], []
    
    print(f"\nFound {len(review_groups)} uncertain groups requiring review")
    print("These records look similar but need confirmation.\n")
    
    approved_groups = []
    rejected_groups = []
    
    for group in review_groups:
        print("\n" + "-" * 80)
        print(f"GROUP {group['group_id'] + 1} of {len(review_groups)}")
        print(f"Confidence: {group.get('confidence', 'uncertain')}")
        print(f"Average Similarity: {group.get('avg_similarity', 0):.1f}%")
        print(f"Number of records: {len(group['records'])}")
        print("-" * 80)
        
        # Show summary of companies this person appears at
        companies = group.get('companies', [])
        if len(companies) > 1:
            print(f"\n  ⚠️  This person appears at MULTIPLE companies:")
            for company in companies:
                print(f"      - {company}")
            print(f"\n  NOTE: If confirmed, contributions from this person will count")
            print(f"        towards ALL companies they're associated with.")
        
        # Display all records in the group
        for idx, record in enumerate(group['records'], 1):
            print(f"\n  Record {idx}:")
            print(f"    Name:     {record.get(name_col, 'N/A')}")
            print(f"    Title:    {record.get(title_col, 'N/A')}")
            print(f"    Company:  {record.get(company_col, 'N/A')}")
            print(f"    Address:  {record.get(address_col, 'N/A')}")
        
        # Ask for confirmation
        while True:
            try:
                if len(companies) > 1:
                    response = input(f"\nAre these ALL records for the SAME PERSON? (yes/no/skip): ").lower().strip()
                else:
                    response = input(f"\nAre these the SAME person? (yes/no/skip): ").lower().strip()
                
                if response in ['yes', 'y']:
                    approved_groups.append(group['group_id'])
                    if len(companies) > 1:
                        print(f"  [APPROVED] Person confirmed - will be linked to {len(companies)} companies")
                    else:
                        print("  [APPROVED] Person confirmed")
                    break
                elif response in ['no', 'n']:
                    rejected_groups.append(group['group_id'])
                    print("  [REJECTED] Group marked as different people")
                    break
                elif response in ['skip', 's']:
                    print("  [SKIPPED] Group will be reviewed later")
                    break
                else:
                    print("  Please enter 'yes', 'no', or 'skip'")
            except (EOFError, KeyboardInterrupt):
                print("\n  [CANCELLED] Review interrupted")
                return approved_groups, rejected_groups
    
    return approved_groups, rejected_groups

def upload_approved_groups_to_firebase(ref, groups: List[Dict], approved_groups: List[int],
                                       name_col: str, title_col: str, address_col: str, company_col: str):
    """
    Upload approved grouped executive records to Firebase.
    Since a person can be at multiple companies, we create:
    1. A person record with all their companies
    2. Individual company-person links for contribution attribution
    """
    print("\n" + "=" * 80)
    print("UPLOADING APPROVED GROUPS TO FIREBASE")
    print("=" * 80)
    
    try:
        executives_ref = ref.child('executives')
        person_companies_ref = ref.child('person_companies')  # For contribution attribution
        
        success_count = 0
        
        for group in groups:
            if group['group_id'] not in approved_groups:
                continue
            
            # For each group, create a single consolidated person record
            records = group['records']
            companies = group.get('companies', [])
            
            # Use the most complete record as the base
            best_record = max(records, key=lambda r: sum(1 for v in r.values() if v and not pd.isna(v)))
            
            # Get person name (normalized)
            person_name = normalize_string(best_record.get(name_col, ""))
            person_name_display = best_record.get(name_col, "")
            
            # Consolidate data from all records in the group
            consolidated = {
                'name': person_name_display,
                'address': best_record.get(address_col, ""),
                'companies': companies,  # All companies this person is associated with
                'grouped_from': len(records),
                'all_variations': []
            }
            
            # Collect all titles across all companies
            titles = set()
            for record in records:
                title = str(record.get(title_col, "")).strip()
                if title and title.lower() not in ['', 'nan', 'none']:
                    titles.add(title)
            if titles:
                consolidated['titles'] = list(titles)
            
            # Add all variations for reference
            for record in records:
                variation = {
                    'name': str(record.get(name_col, "")),
                    'title': str(record.get(title_col, "")),
                    'company': str(record.get(company_col, "")),
                    'address': str(record.get(address_col, ""))
                }
                consolidated['all_variations'].append(variation)
            
            # Upload person record to Firebase (keyed by normalized name)
            name_key = person_name.replace(' ', '_').replace(',', '').replace('.', '')
            executives_ref.child(name_key).set(consolidated)
            
            # Create company-person links for contribution attribution
            # This allows contributions from this person to count towards all their companies
            for company in companies:
                company_key = normalize_string(company).replace(' ', '_').replace(',', '').replace('.', '')
                # Store under /person_companies/[company]/[person_name] = true
                person_companies_ref.child(company_key).child(name_key).set({
                    'person_name': person_name_display,
                    'linked_at': datetime.now().isoformat()
                })
            
            success_count += 1
            if len(companies) > 1:
                print(f"  [SUCCESS] Uploaded person: {person_name_display}")
                print(f"            Linked to {len(companies)} companies: {', '.join(companies[:3])}{'...' if len(companies) > 3 else ''}")
            else:
                print(f"  [SUCCESS] Uploaded person: {person_name_display} at {companies[0] if companies else 'N/A'}")
        
        print(f"\nSUCCESS: Uploaded {success_count} approved person groups to Firebase")
        print(f"         Person records stored at: /executives/[person_name]")
        print(f"         Company links stored at: /person_companies/[company]/[person_name]")
        print(f"\nNOTE: Contributions from each person will count towards ALL their companies")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to upload to Firebase: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function to run the executive review tool"""
    print("EXECUTIVE RECORD REVIEW TOOL")
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
        
        # Step 3: Get executive records
        print("\nStep 3: Retrieving executive records from Index Align database...")
        df = get_executives_from_database(conn)
        if df is None or df.empty:
            print("ERROR: No executive records retrieved from database")
            return False
        
        # Step 4: Identify column names
        print("\nStep 4: Identifying column names...")
        name_col, title_col, address_col, company_col = identify_column_names(df)
        
        if not name_col:
            print("ERROR: Could not identify name column")
            print(f"Available columns: {list(df.columns)}")
            return False
        
        print(f"Identified columns:")
        print(f"  Name: {name_col}")
        print(f"  Title: {title_col or 'NOT FOUND'}")
        print(f"  Address: {address_col or 'NOT FOUND'}")
        print(f"  Company: {company_col or 'NOT FOUND'}")
        
        # Step 5: Group executive records
        print("\nStep 5: Grouping executive records...")
        groups, uncertain_groups = group_executive_records(
            df, name_col, title_col or name_col, address_col or name_col, company_col or name_col
        )
        
        # Step 6: Export uncertain groups to JSON
        print("\nStep 6: Exporting uncertain groups to JSON file...")
        json_filename, export_data = export_review_data(
            groups, uncertain_groups, df, name_col, title_col or name_col,
            address_col or name_col, company_col or name_col
        )
        
        if json_filename:
            print(f"  [SUCCESS] Exported review data to: {os.path.abspath(json_filename)}")
            
            # Try to open the file automatically
            try:
                import platform
                system = platform.system()
                if system == 'Darwin':  # macOS
                    os.system(f'open "{os.path.abspath(json_filename)}"')
                elif system == 'Windows':
                    os.system(f'start "" "{os.path.abspath(json_filename)}"')
                elif system == 'Linux':
                    os.system(f'xdg-open "{os.path.abspath(json_filename)}"')
            except Exception as e:
                pass
        
        # Step 7: Display review interface
        print("\nStep 7: Starting manual review...")
        approved_groups, rejected_groups = display_review_interface(
            groups, uncertain_groups, name_col, title_col or name_col,
            address_col or name_col, company_col or name_col
        )
        
        # Step 8: Upload approved groups
        if approved_groups:
            print(f"\nStep 8: Uploading {len(approved_groups)} approved groups...")
            upload_approved_groups_to_firebase(
                firebase_ref, groups, approved_groups,
                name_col, title_col or name_col, address_col or name_col, company_col or name_col
            )
        else:
            print("\nNo groups were approved for upload")
        
        print("\nSUCCESS: Executive review tool completed")
        return True
        
    except Exception as e:
        print(f"\nERROR: Pipeline failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        print("\nClosing connections...")
        if conn:
            conn.close()
            print("SUCCESS: Database connection closed")
        if tunnel:
            tunnel.stop()
            print("SUCCESS: SSH tunnel closed")

if __name__ == "__main__":
    main()

