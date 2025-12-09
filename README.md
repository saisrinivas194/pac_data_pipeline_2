# Index Align to Firebase Data Pipeline

Data pipelines for transferring data from Index Align database to Firebase Realtime Database.

## Pipelines

1. **Issues Pipeline** - Transfers issues data (automatic, no manual review needed)
2. **Executive Review Tool** - Groups and reviews executive records (requires manual confirmation)

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup Environment Variables

```bash
cp ENVIRONMENT_TEMPLATE.txt .env
# Edit .env with your real credentials
```

Required environment variables:
- **Firebase**: Project ID, Private Key, Client Email, etc.
- **Index Align**: SSH host, database credentials, etc.

See `ENVIRONMENT_TEMPLATE.txt` for all required variables.

### 3. Test Connections

```bash
python test_index_align.py
```

This will test:
- [OK] Environment variables are set
- [OK] SSH connection to Index Align
- [OK] Database connection through SSH tunnel
- [OK] Firebase connection

### 4. Run the Pipelines

#### Issues Pipeline (Automatic)

```bash
python index_align_to_firebase.py
```

The issues pipeline automatically transfers issues data and PAC data. No manual review needed.

#### Executive Review Tool (Manual Review Required)

```bash
python executive_review_tool.py
```

The executive review tool:
1. Retrieves executive records from Index Align
2. **Groups records that look like the same person at the same company** using fuzzy matching
3. **Only highlights uncertain matches** (not everything - just confusing ones)
4. Matches based on: names, job titles, addresses, company names
5. Provides a simple review interface where you confirm or reject each group
6. Uploads only approved groups to Firebase

**Important for Executive Review:**
- The tool does the heavy matching work automatically
- You only review uncertain matches (typically 75-85% similarity)
- High confidence matches (>85%) are auto-approved
- Low confidence matches (<75%) are not grouped
- For each uncertain group, you'll see all records and confirm if they're the same person

## Issues Pipeline - What It Does

1. **Connects to Firebase** - Uses service account credentials
2. **Creates SSH Tunnel** - Securely connects to Index Align database
3. **Retrieves Issues Data** - Reads all rows from `issues` table
4. **Transforms Data** - Converts ticker → company_id and structures as:
   ```
   /issues/[company_id]/[issue_name]/Against, Neutral, Pro
   ```
5. **Exports All Data to JSON** - Creates a complete JSON file with ALL data for review:
   - All companies with all their issues
   - Complete Against, Neutral, Pro values for each issue
   - Position calculations (AGAINST/PRO/NEUTRAL)
   - File automatically opens in your default JSON viewer
   - File saved as `issues_review_YYYYMMDD_HHMMSS.json`
6. **Manual Approval Required** - User must review JSON file and explicitly approve before upload
7. **Uploads to Firebase** - Only after approval, stores in nested structure, overwrites entire company objects

## Executive Review Tool - What It Does

**Important:** Executives can be at multiple companies (e.g., Elon Musk at Tesla, SpaceX, Twitter). 
Contributions from that person should count towards ALL companies they're an executive of.

1. **Connects to Firebase** - Uses service account credentials
2. **Creates SSH Tunnel** - Securely connects to Index Align database
3. **Retrieves Executive Records** - Reads all rows from `executives` table (or similar)
4. **Groups by PERSON (across all companies)** - Uses fuzzy matching to identify records that look like the same person:
   - **Name matching** (50% weight) - Most important, handles variations like "John Smith" vs "J. Smith"
   - **Address matching** (25% weight) - Strong indicator of same person
   - **Title matching** (15% weight) - Can help but less important
   - **Company matching** (10% weight) - Least important since person can be at multiple companies
   - Groups records that are the SAME PERSON regardless of company
5. **Tracks All Companies** - For each person, identifies all companies they're associated with
6. **Identifies Uncertain Matches** - Only highlights groups with 75-85% similarity for review
   - High confidence (>85%): Auto-approved, no review needed
   - Uncertain (75-85%): Requires manual review
   - Low confidence (<75%): Not grouped
7. **Exports Review Data** - Creates JSON file with only uncertain groups:
   - File saved as `executive_review_YYYYMMDD_HHMMSS.json`
   - Contains only groups that need review
   - Shows all companies each person is associated with
8. **Interactive Review** - Simple interface where you:
   - See all records in each uncertain group
   - See all companies the person appears at (if multiple)
   - Confirm if they're the same person (yes/no/skip)
   - Tool does the heavy work, you just confirm
9. **Uploads Approved Groups** - Creates:
   - Person records at `/executives/[person_name]` with all their companies
   - Company-person links at `/person_companies/[company]/[person_name]` for contribution attribution
   - Contributions from each person count towards ALL their companies

## Data Structures

### Issues Data Structure

The issues pipeline transforms data into this Firebase structure:

```
/issues/
  [company_id]/
    [Issue Name]/
      Against: (float)
      Neutral: (float)
      Pro: (float)
```

- Each company should have exactly 8 issues
- Uses ticker → company_id mapping from Firebase `/tickers`
- Overwrites entire company object on each upload

### Executive Data Structure

The executive review tool uploads person records and company links:

```
/executives/
  [person_name]/
    name: (string)
    address: (string)
    companies: (array) - ALL companies this person is an executive of
    titles: (array) - All job titles across companies
    grouped_from: (int) - number of records that were grouped
    all_variations: (array) - all original record variations

/person_companies/
  [company_name]/
    [person_name]/
      person_name: (string)
      linked_at: (timestamp)
```

**Key Points:**
- Only approved groups are uploaded
- Each person gets ONE record with ALL their companies listed
- Company-person links allow contributions to be attributed to all companies
- If Elon Musk makes a contribution listed under "Tesla", it counts for Tesla, SpaceX, AND Twitter
- Original variations are preserved for reference

## Files

- `index_align_to_firebase.py` - Issues pipeline script (automatic)
- `executive_review_tool.py` - Executive review tool (manual review)
- `test_index_align.py` - Connection testing script
- `requirements.txt` - Python dependencies
- `ENVIRONMENT_TEMPLATE.txt` - Template for .env file
- `README.md` - This file

## Requirements

- Python 3.8 or newer
- Firebase Realtime Database enabled
- SSH access to Index Align server
- Database credentials

## Troubleshooting

**Can't connect via SSH?**
- Check SSH key permissions: `chmod 600 ~/.ssh/id_rsa`
- Test manually: `ssh ubuntu@indexalign`

**Database errors?**
- Verify database is running on server
- Check credentials in `.env` file

**Firebase errors?**
- Verify service account has Realtime Database permissions
- Check project ID matches in `.env`

**Missing packages?**
```bash
pip install --upgrade -r requirements.txt
```

## Security Notes

- Never commit your `.env` file
- Use SSH keys instead of passwords
- Consider using a read-only database user

## Support

For detailed setup instructions, see the setup guide or check the code comments.

