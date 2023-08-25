# Chapter1
```bash
git --version
pwd
ls
mkdir archive
cd archive
echo aaa > todo.txt  # Create a new file with contents
echo bbb >> todo.txt  # Add contents to an existing file
ls -l
ls -a
```

## Git workflow
| Working Directory | Staging Area | Permanent Storage |
| :---: | :---: | :---: |
| modify a file | save the draft | commit the updated file |

```bash
echo # xxx > report.md
git add report.md
git status
git commit -m "add title"
git status
```

### Compare an `unstaged` file with the last committed version
```bash
echo - aaa >> report.md
git status
git add .
git status
git commit -m "add aaa"
echo - bbb >> report.md
git status
git diff report.md  
```

### Compare a `staged` file with the last committed version
```bash
echo - ccc >> report.md
git add report.md
# git commit -m "add ccc"
git status
git diff -r HEAD report.md
```

### Compare `all staged` files with the last committed versions
```bash
touch sales.csv
git add sales.csv
git diff -r HEAD
```
# Chapter2
## Commit structure
| Commit | Tree | Blob |
| :---: | :---: | :---: |
| commit_id | files | contents |

## Git log
```bash
git log  # press q to quit
git log -<n>
git log -<n> <file>
git log --since='<Month> <Day> <Year>' --until='<Month> <Day> <Year>'  # 'Apr 11 2022'
git show <commit_id>

git diff -r HEAD~1
git diff -r HEAD~2
git diff -r HEAD~3

git diff <commit_id1> <commit_id2>
```

## Undoing changes before committing
```bash
git reset HEAD
git reset HEAD <file_name>
```

## Undo changes to an unstaged file
```bash
git checkout .
git checkout -- <unstaged_file>
```

## Unstaging and undoing
```bash
git reset HEAD
git checkout .
git add . 
git commit -m "<comment>"
```

## Restoring an old version of a file
```bash
git checkout <commit_id>
git checkout <commit_id> <file>
```

## Cleaning a repository
```bash
git clean -n 
git clean -f  # This command cannot be undone!
```

## Ignoring specific files
```bash
echo *.log >> .gitignore
```

## Branches
```bash
git branch
git branch <new_branch_name>
git checkout <branch_name>
git checkout -b <new_branch_name>

git branch -d <branch_name>  # Delete branch `locally`
git push origin --delete  <branch_name>  # Delete branch `remotely`
git push origin :<branch_name>  # A shorter command to delete a branch remotely

# WARNING: If you get the error below, it may mean that someone else has already deleted the branch.
git fetch -p  # The -p flag means "prune". After fetching, branches which no longer exist on the remote will be deleted.

git diff <branch_name1> <branch_name2> 
```

## Merging branches
```bash
git merge <source> <destination>  # Merge `source` into `destination`
```

## Handling conflict
```bash
branch: main
A) Write report. 
B) Submit report. 

touch todo.txt
git add todo.txt
git commit -m "add todo.txt"
git checkout -b update

branch: update
A) Write report. 
B) Submit report. 
C) Submit expenses. 

git add todo.txt
git commit -m "add todo.txt"
git checkout main
```

## Creating a custom alias
```bash
git config --global alias.ci 'commit -m'
git config --global alias.unstage 'reset HEAD'
```

## Creating a remote
```bash
git remote  # When cloning, Git will automatically name the remote `origin`
git remote -v
git remote add <name> <repository_url>
```

--------------------------
# Demo
- demo-repo
  - file:
    - README.md
    ```
      # Demo
        - aaa
    ```
    - index.html
      ```
      <div>Hello<div>
      <div>World<div>  branch: quick-test >> # git commit -am "add World"
      <div>There<div>  branch: main >> # git commit -am "add There"
      ```
    - image.jpg
  - branch:
    - main
    - feature-01
    - quick-test
- demo-repo2
  - README.md


## branch
```bash
git branch
git branch <branch_name>
git checkout <branch_name>
git checkout -b <branch_name>
git branch -d <branch_name>
git push origin -d <branch_name>

>>> README.md
git checkout -b feature-01
(edit README.md)
git add README.md
git commit -m "update readme"
git checkout main
git diff feature-01

# git merge feature-01
git checkout feature-01
git push
git push -u origin main
(do a Pool Request)

git checkout main
git pull

git branch -d feature-01
git puch origin -d feature-01
--------------------------------
>>> index.html
  <div>Hello<div>
  <div>world<div>

git checkout -b quick-test
git status
git commit -am "add world"

git checkout main
>>> index.html
  <div>Hello<div>
  <div>there<div>
git checkout quick-test -> error! (commit or stash)
git commit -am "add there"

git checkout quick-test
git diff main
git merge main -> error! (accept both changes)
git status
git diff
git commit -am "merge into main"
```

## mistake
```bash
>>> README.md
(edit README.md)
  - xxx
git status
git add README.md
git status
git reset HEAD
git status

git add README.md
git commit -m "add xxx"
git status
git reset HEAD~1
git status

git log
git reset <commit_id>
```