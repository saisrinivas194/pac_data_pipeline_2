# Index Align to Firebase Issues Pipeline

Professional data pipeline for transferring issues data from Index Align database to Firebase Realtime Database.

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

### 4. Run the Pipeline

```bash
python index_align_to_firebase.py
```

The pipeline will:
1. Connect to Index Align and retrieve data
2. Transform the data
3. **Display a comprehensive visualization** for you to review
4. **Ask for your explicit approval** before uploading
5. Only upload if you approve (type 'yes')

**Important:** 
1. The script will export ALL data to a JSON file (e.g., `issues_review_20241123_143022.json`)
2. The JSON file will automatically open in your default application
3. You must review ALL the data in the JSON file
4. Return to the terminal and type 'yes' to approve the upload
5. This prevents accidental uploads by requiring manual review of all data

## What It Does

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

## Data Structure

The pipeline transforms data into this Firebase structure:

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

## Files

- `index_align_to_firebase.py` - Main pipeline script
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

