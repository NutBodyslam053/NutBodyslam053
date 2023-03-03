# Create a new Bucket
gsutil mb gs://[BUCKET]

# List files in bucket
gsutil ls gs://[BUCKET]

# Check bucket usage (du: disk usage)
gsutil du -sh gs://[BUCKET]

# Copy (upload) file to bucket
gsutil cp [File] gs://[BUCKET]
# > no need to create directory
gsutil cp [File] gs://[BUCKET]/path/to/file
# > copy (upload) directory
gsutil cp -r [Folder] gs://[BUCKET]
# > enable multiprocessing
gsutil -m cp -r [Folder] gs://[BUCKET]
# > copy (download) file
gsutil cp gs://[BUCKET]/path/to/file [File]
# > copy (download) directory to current local directory
gsutil -m cp -r gs://[BUCKET]/path .
# > copy (download) txt files to current local directory
gsutil -m cp gs://[BUCKET]/*.txt . 

# Move or rename object
gsutil mv gs://[BUCKET]/path/to/old_name gs://[BUCKET]/path/to/new_name

# Remove file or directory 
# > remove file
gsutil rm gs://[BUCKET]/path/to/file
# > remove directory
gsutil rm -r gs://[BUCKET]/path/to/directory


# Doc: https://cloud.google.com/storage/docs/how-to