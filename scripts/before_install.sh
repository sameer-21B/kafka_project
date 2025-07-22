#!/bin/bash
echo "BeforeInstall hook: Cleaning old ETL script files if necessary."
# Optional: You might want to stop any long-running ETL processes if they shouldn't be interrupted.
# This is often handled by making the ETL process idempotent or using locking.
rm -rf /home/ec2_user/db_ingest/* || true
