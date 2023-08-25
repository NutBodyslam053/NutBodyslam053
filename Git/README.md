
1. Set up the github repository

```bash
    echo "# <project_name>" >> README.md
    git init
    git add README.md
    git commit -m "first commit"
    git branch -M main
    git remote add origin <git_repository>
    git push -u origin main
```
2. Set up Git configuration (First time)
```bash
    git config --global user.name <user_name>
    git config --global user.email <email>
```

3. Setup environment
```bash
    conda create -p venv python=3.9 -y
    conda activate venv/
    touch .gitignore setup.py requirements.txt
    mkdir src & type nul > src/__init__.py
    pip install -r requirements.txt
```