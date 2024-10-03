#!/bin/bash

TIMESTAMP=$(date +"%Y%m%d%H%M")
BACKUP_DIR="/backups"
#DB_HOST="postgres" # or use 'localhost' if it's running on the same container
#DB_USER="postgres"
#PGPASSWORD=$POSTGRES_PASSWORD

pg_dumpall -h $DB_HOST -p 5432 -U $DB_USER -f $BACKUP_DIR/backup_$TIMESTAMP.sql
