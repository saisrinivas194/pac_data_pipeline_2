# GitHub Commands Guide

## Initial Setup (First Time)

### 1. Stage all files for commit
```bash
git add .
```

Or add specific files:
```bash
git add .gitignore
git add README.md
git add index_align_to_firebase.py
git add test_index_align.py
git add requirements.txt
git add ENVIRONMENT_TEMPLATE.txt
git add whatsapp_workflow_message.txt
```

### 2. Make your first commit
```bash
git commit -m "Initial commit: Index Align to Firebase pipeline"
```

### 3. Create a new repository on GitHub
- Go to https://github.com/new
- Create a new repository (e.g., `index-align-firebase-pipeline`)
- **DO NOT** initialize with README, .gitignore, or license (you already have these)

### 4. Connect your local repository to GitHub
```bash
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
```

Replace:
- `YOUR_USERNAME` with your GitHub username
- `YOUR_REPO_NAME` with your repository name

### 5. Push to GitHub
```bash
git branch -M main
git push -u origin main
```

---

## Daily Workflow Commands

### Check status
```bash
git status
```

### See what changed
```bash
git diff
```

### Stage changes
```bash
git add .
# or
git add <specific-file>
```

### Commit changes
```bash
git commit -m "Description of your changes"
```

### Push to GitHub
```bash
git push
```

### Pull latest changes (if working with others)
```bash
git pull
```

---

## Useful Commands

### View commit history
```bash
git log
```

### View remote repositories
```bash
git remote -v
```

### Create a new branch
```bash
git checkout -b feature-branch-name
```

### Switch branches
```bash
git checkout main
```

### Merge a branch
```bash
git checkout main
git merge feature-branch-name
```

### Undo last commit (keep changes)
```bash
git reset --soft HEAD~1
```

### See what files are tracked/ignored
```bash
git ls-files
```

---

## Quick Start (Copy-Paste Ready)

```bash
# Navigate to project directory
cd "/Users/saisrinivaspedhapolla/Downloads/goods unite us/new"

# Stage all files
git add .

# Commit
git commit -m "Initial commit: Index Align to Firebase pipeline"

# Add remote (replace with your GitHub repo URL)
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git

# Push to GitHub
git branch -M main
git push -u origin main
```

---

## Important Notes

⚠️ **Never commit these files:**
- `.env` (contains secrets - already in .gitignore)
- Any files with API keys or passwords
- `__pycache__/` directories
- `.DS_Store` files

✅ **Safe to commit:**
- `.gitignore` ✓
- `README.md` ✓
- `*.py` files ✓
- `requirements.txt` ✓
- `ENVIRONMENT_TEMPLATE.txt` ✓ (template only, no secrets)

---

## Troubleshooting

### If you get "remote origin already exists"
```bash
git remote remove origin
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
```

### If you need to update remote URL
```bash
git remote set-url origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
```

### If push is rejected
```bash
git pull origin main --rebase
git push
```

### Check if .env is being tracked (should NOT be)
```bash
git check-ignore .env
# Should output: .env
```





